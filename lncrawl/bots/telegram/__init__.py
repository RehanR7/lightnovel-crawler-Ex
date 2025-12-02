import asyncio
import logging
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Any
from urllib.parse import urlparse, urlsplit
import validators

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    Job,
)

from lncrawl.core.app import App
from lncrawl.core.sources import prepare_crawler
from lncrawl.utils.uploader import upload

logger = logging.getLogger(__name__)

# Constants for better maintainability
available_formats = ["epub", "text", "web", "mobi", "pdf"]
MAX_FILE_SIZE = 49.99 * 1024 * 1024  # 49.99 MB
SESSION_TIMEOUT = 300  # 5 minutes

# User response constants
USER_RESPONSE_YES = "yes"
USER_RESPONSE_NO = "no"

class TelegramBot:
    def __init__(self):
        self.executor = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix="telegram_bot"
        )
        self.active_sessions: Dict[str, dict] = {}  # Track active user sessions
        self.max_active_sessions = 5  # Maximum concurrent sessions
        self.last_request_time = {}  # For rate limiting

    def start(self):
        os.environ["debug_mode"] = "yes"

        # Build the Application and with bot's token.
        TOKEN = os.getenv("TELEGRAM_TOKEN", "")
        if not TOKEN:
            raise Exception("Telegram token not found")

        self.application = Application.builder().token(TOKEN).build()
        self.application.add_handler(CommandHandler("help", self.show_help))
        self.application.add_handler(CommandHandler("status", self.handle_downloader))
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", self.init_app),
                MessageHandler(
                    filters.TEXT & ~(filters.COMMAND), self.handle_novel_url
                ),
            ],
            fallbacks=[CommandHandler("cancel", self.destroy_app)],
            states={
                "handle_novel_url": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_novel_url
                    ),
                ],
                "handle_crawler_to_search": [
                    CommandHandler("skip", self.handle_crawler_to_search),
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_crawler_to_search
                    ),
                ],
                "handle_select_novel": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_select_novel
                    ),
                ],
                "handle_select_source": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_select_source
                    ),
                ],
                "handle_delete_cache": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_delete_cache
                    ),
                ],
                "handle_range_selection": [
                    CommandHandler("all", self.handle_range_all),
                    CommandHandler("last", self.handle_range_last),
                    CommandHandler("first", self.handle_range_first),
                    CommandHandler("volume", self.handle_range_volume),
                    CommandHandler("chapter", self.handle_range_chapter),
                    MessageHandler (
                        filters.TEXT & ~(filters.COMMAND),
                        self.display_range_selection_help,
                    ),
                ],
                "handle_volume_selection": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_volume_selection
                    ),
                ],
                "handle_chapter_selection": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_chapter_selection
                    ),
                ],
                "handle_pack_by_volume": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_pack_by_volume
                    ),
                ],
                "handle_output_format": [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND), self.handle_output_format
                    ),
                ],
            },
            conversation_timeout=SESSION_TIMEOUT,
        )
        self.application.add_handler(conv_handler)

        # Fallback helper
        self.application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_downloader)
        )

        # Log all errors
        self.application.add_error_handler(self.error_handler)
        print("Telegram bot is online!")

        # Run the bot until you press Ctrl-C or the process receives SIGINT,
        # SIGTERM or SIGABRT. This should be used most of the time, since
        # start_polling() is non-blocking and will stop the bot gracefully.
        # Start the Bot
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.warning(f"Error: {context.error}\nCaused by: {update}")
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="😅 An unexpected error occurred. Please try again or use /cancel to restart."
                )
            except Exception as send_error:
                logger.error(f"Failed to send error message: {send_error}")

    async def rate_limit_check(self, chat_id: str) -> bool:
        """Check if user is sending requests too frequently"""
        current_time = time.time()
        last_time = self.last_request_time.get(chat_id, 0)
        
        # Allow one request per 2 seconds
        if current_time - last_time < 2:
            return False
        
        self.last_request_time[chat_id] = current_time
        return True

    async def validate_session(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[bool, Optional[dict], Optional[App]]:
        """Validate session existence and return session and app objects"""
        if not update.effective_message:
            return False, None, None
            
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Your session has expired. Please start a new session with /start.")
            return False, None, None
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start a new session with /start.")
            await self.destroy_app(update, context)
            return False, None, None
            
        return True, session, app

    async def show_help(self, update: Update, _: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_markdown(
            "_🤖 Available Commands:_ \n\n"
            "- /start - Create a new session\n"
            "- /status - Check current download status\n"
            "- /cancel - Stop current session",
        )
        return ConversationHandler.END

    def get_current_jobs(self, chat_id: str, context: ContextTypes.DEFAULT_TYPE):
        return context.job_queue.get_jobs_by_name(
            chat_id
        ) + context.job_queue.get_jobs_by_name(f"{chat_id}_progress")

    async def destroy_app(self, update: Update = None, context: 
        ContextTypes.DEFAULT_TYPE = None, job=None
    ):
        # Determine chat_id correctly for both handler and job queue
        if update is not None:
            chat_id = str(update.effective_message.chat_id)
        else:
            job = job or (context.job if hasattr(context, "job") else None)
            if not job:
                logger.error("No update or job provided to destroy_app")
                return ConversationHandler.END
            chat_id = str(job.chat_id)
        
        # Get session with validation
        session = self.active_sessions.get(chat_id)
        if not session:
            logger.info(f"No session found for chat_id {chat_id} to destroy")
            return ConversationHandler.END
        
        # Cancel running futures with timeout
        for future_key in ["download_future", "bind_future"]:
            future = session.get(future_key)
            if future and not future.done():
                future.cancel()
                logger.info(f"Cancelled {future_key} for chat_id {chat_id}")
                # Optional: wait briefly for cancellation
                try:
                    await asyncio.wait_for(
                        asyncio.shield(future), 
                        timeout=2.0
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
        
        # Remove scheduled jobs
        for job in self.get_current_jobs(chat_id, context):
            job.schedule_removal()
            logger.info(f"Removed job {job.name} for chat_id {chat_id}")
        
        # Destroy app instance
        app = session.get("app")
        if app:
            try:
                app.destroy()
            except Exception as e:
                logger.exception(
                    "Failed to destroy app for chat_id %s: %s",
                    chat_id,
                    e,
                )
            finally:
                self.active_sessions.pop(chat_id, None)
                logger.info("Session destroyed for chat_id: %s", chat_id)
        
        # Notify the user (with error handling)
        try:
            if update:
                await update.message.reply_text(
                    "Session closed. Tap /start to open a new session.",
                    reply_markup=ReplyKeyboardRemove()
                )
            elif context:
                await context.bot.send_message(
                    chat_id,
                    text="Session closed due to inactivity. Tap /start to open a new session.",
                    reply_markup=ReplyKeyboardRemove()
                )
        except Exception as e:
            logger.warning(
                f"Could not send session closure message to {chat_id}: {e}"
            )
        
        return ConversationHandler.END

    async def init_app(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.rate_limit_check(str(update.effective_message.chat_id)):
            await update.message.reply_text("⏳ Please wait a moment before starting a new session.")
            return ConversationHandler.END

        chat_id = str(update.effective_message.chat_id)

        # Check if user already has an active session
        if chat_id in self.active_sessions:
            await update.message.reply_markdown(
                "🔴 *Active Session Found*\n\n"
                "You already have an active session.\n"
                "Please send /cancel to close it before starting a new one."
            )
            return ConversationHandler.END

        # Check session limit
        if len(self.active_sessions) >= self.max_active_sessions:
            await update.message.reply_markdown(
                "🔴 *Too Many Requests*\n\n"
                "Sorry, the bot is currently handling *too many requests*.\n"
                "Please try again later 🕒"
            )
            return ConversationHandler.END

        try:
            app = App()
        except Exception as e:
            logger.exception("Failed to create App instance: %s", e)
            await update.message.reply_text("Failed to initialize the application. Please try again later.")
            return ConversationHandler.END

        root = os.path.abspath(".telegram_bot_output")
        good_name = os.path.basename(app.output_path)
        output_path = os.path.join(root, chat_id, good_name)
        
        # Create output directory asynchronously
        try:
            await asyncio.to_thread(os.makedirs, os.path.dirname(output_path), exist_ok=True)
        except Exception as e:
            logger.exception("Failed to create output directory: %s", e)
            await update.message.reply_text("Failed to create working directory. Please try again later.")
            return ConversationHandler.END

        app.output_path = output_path

        self.active_sessions[chat_id] = {
            "app": app,
            "status": "Initialized",
            "error": None,
            "last_activity": time.time(),
        }
        await update.message.reply_text(
            "✅ *New Session Created*\n\n"
            "I'm ready to help you download webnovels!",
            parse_mode=ParseMode.MARKDOWN
        )
        await update.message.reply_text(
            "📚 *How to use:*\n\n"
            "I can work with the following types of input:\n"
            "• *Profile URL* - Direct link to a webnovel's profile page\n"
            "• *Search Query* - Just type the name of the webnovel you're looking for\n\n"
            "Type your input below or send /cancel to stop.",
            parse_mode=ParseMode.MARKDOWN
        )
        return "handle_novel_url"

    async def _validate_url(self, url: str) -> bool:
        """Validate URL format and safety"""
        if not validators.url(url):
            return False
            
        # Block private/reserved IP ranges and localhost
        parsed = urlsplit(url)
        hostname = parsed.hostname or ""
        
        # Block localhost and private IP ranges
        if hostname in ["localhost", "127.0.0.1", "::1"]:
            return False
            
        # Block private IP ranges
        if re.match(r'^(10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|192\.168\.|169\.254\.)', hostname):
            return False
            
        # Block file protocol
        if parsed.scheme == "file":
            return False
            
        return True

    async def handle_novel_url(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not update.message:
            return ConversationHandler.END
            
        chat_id = str(update.effective_message.chat_id)
        
        # Rate limiting
        if not await self.rate_limit_check(chat_id):
            await update.message.reply_text("⏳ Please wait a moment before sending another request.")
            return "handle_novel_url"

        session = self.active_sessions.get(chat_id)

        if not session:
            await update.message.reply_markdown(
                "🔴 *Session Not Found*\n\n"
                "Please start a new session with /start command."
            )
            return ConversationHandler.END

        if self.get_current_jobs(chat_id, context):
            app = session.get("app")
            status = session.get("status", "Processing...")
            await update.message.reply_markdown(
                f"*{status}*\n\n"
                f"*{int(app.progress)}* out of *{len(app.chapters)}* chapters has been downloaded.\n\n"
                "To terminate this session, send /cancel command."
            )
            return "handle_novel_url"

        app = session.get("app", App())
        session["app"] = app
        user_input = update.message.text.strip()
        app.user_input = user_input
        
        # Update last activity time
        session["last_activity"] = time.time()

        # Validate URL if it looks like one
        if user_input.startswith(("http://", "https://")) and not await self._validate_url(user_input):
            await update.message.reply_text(
                "⚠️ Invalid or unsafe URL. Please provide a valid webnovel URL or search query."
            )
            return "handle_novel_url"

        try:
            await update.message.reply_text("🔍 Processing your request...")
            await asyncio.to_thread(app.prepare_search)
        except Exception as e:
            logger.exception("Failed to init crawler for chat_id %s: %s", chat_id, e)
            await update.message.reply_markdown(
                "🔴 *Failed to init crawler*\n\n"
                "Sorry! I only recognize these "
                "[supported sources](https://github.com/dipu-bd/lightnovel-crawler#supported-sources).\n"
                "Enter something again or send /cancel command to stop.\n"
                "You can send the novelupdates link of the novel too."
            )
            return "handle_novel_url"

        if app.crawler:
            await update.message.reply_text("✅ Got your page link")
            return await self.get_novel_info(update, context)

        if len(app.user_input) < 5:
            await update.message.reply_text(
                "🔍 Please enter a longer query text (at least 5 letters)."
            )
            return "handle_novel_url"

        await update.message.reply_text("✅ Got your query text")
        return await self.show_crawlers_to_search(update, context)

    async def show_crawlers_to_search(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)

        # Update last activity time
        session["last_activity"] = time.time()
        
        buttons = []

        def make_button(i, url):
            return "%d - %s" % (i + 1, urlparse(url).hostname)

        # Create buttons in pairs for better UI
        for i in range(0, len(app.crawler_links), 2):
            row = [make_button(i, app.crawler_links[i])]
            if i + 1 < len(app.crawler_links):
                row.append(make_button(i + 1, app.crawler_links[i + 1]))
            buttons.append(row)

        await update.message.reply_markdown(
            "Choose the *source* to search for your novel, \n"
            "or send /skip to search *everywhere*.",
            reply_markup=ReplyKeyboardMarkup(buttons, one_time_keyboard=True),
        )
        return "handle_crawler_to_search"

    async def handle_crawler_to_search(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()

        link = update.message.text.strip()
        if link != "/skip":
            selected_crawlers = []
            if link.isdigit():
                idx = int(link) - 1
                if 0 <= idx < len(app.crawler_links):
                    selected_crawlers.append(app.crawler_links[idx])
            else:
                # Match by the button text
                for i, url in enumerate(app.crawler_links):
                    button_text = f"{i + 1} - {urlparse(url).hostname}"
                    if link == button_text:
                        selected_crawlers.append(url)
                        break
            
            if selected_crawlers:
                app.crawler_links = selected_crawlers
            else:
                await update.message.reply_text("Invalid selection. Please choose a source from the list or send /skip.")
                return "handle_crawler_to_search"

        await update.message.reply_markdown(
            f'Searching for *"{app.user_input}"* in {len(app.crawler_links)} sites. Please wait.',
            reply_markup=ReplyKeyboardRemove(),
        )
        await update.message.reply_markdown(
            "⏳ _DO NOT type anything until I reply._\n"
            "You can only send /cancel to stop this session."
        )

        # Run search in a separate thread with timeout
        try:
            future = self.executor.submit(app.search_novel)
            await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(future)), timeout=60.0)
        except asyncio.TimeoutError:
            future.cancel()
            logger.warning("Search operation timed out for chat_id %s", chat_id)
            await update.message.reply_text(
                "🔍 Search operation timed out. This can happen with slow websites.\n"
                "Please try with a different source or a more specific search query."
            )
            return "handle_novel_url"
        except Exception as e:
            logger.exception("Search failed for chat_id %s: %s", chat_id, e)
            await update.message.reply_text(f"Search failed: {str(e)}")
            return await self.destroy_app(update, context)

        if not app.search_results:
            await update.message.reply_text(
                "🤷‍♂️ No results found for your query.\n"
                "Try a different search term or send a direct URL to the novel."
            )
            return "handle_novel_url"

        return await self.show_novel_selection(update, context)

    async def show_novel_selection(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()

        if len(app.search_results) == 0:
            await update.message.reply_text(
                "🤷‍♂️ No results found by your query.\nTry again or send /cancel to stop."
            )
            return "handle_novel_url"

        if len(app.search_results) == 1:
            session["selected"] = app.search_results[0]
            return await self.show_source_selection(update, context)

        # Create selection buttons
        buttons = []
        for i, res in enumerate(app.search_results):
            title = res['title'].replace('\n', ' ').strip()
            button_text = f"{i + 1}. {title[:40]}{'...' if len(title) > 40 else ''}"
            buttons.append([button_text])

        await update.message.reply_text(
            "📚 Choose any one of the following novels, or send /cancel to stop this session.",
            reply_markup=ReplyKeyboardMarkup(buttons, one_time_keyboard=True),
        )
        return "handle_select_novel"

    async def handle_select_novel(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()

        text = update.message.text.strip()
        selected = None
        if text.isdigit():
            idx = int(text) - 1
            if 0 <= idx < len(app.search_results):
                selected = app.search_results[idx]
        else:
            # Try to match the button text
            for i, item in enumerate(app.search_results):
                title = item['title'].replace('\n', ' ').strip()
                button_text = f"{i + 1}. {title[:40]}{'...' if len(title) > 40 else ''}"
                if text == button_text:
                    selected = item
                    break

                # Fallback: partial match with title
                if len(text) >= 5 and text.lower() in title.lower():
                    selected = item
                    break

        if not selected:
            await update.message.reply_text(
                "❌ Please select a valid novel from the list."
            )
            return await self.show_novel_selection(update, context)

        session["selected"] = selected
        return await self.show_source_selection(update, context)

    async def show_source_selection(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        selected = session.get("selected")
        if not selected:
            await update.message.reply_text("Selection data lost. Please start again.")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        if len(selected["novels"]) == 1:
            app = session.get("app")
            if not app:
                await update.message.reply_text("Session data is corrupted. Please start again with /start")
                return await self.destroy_app(update, context)
                
            app.crawler = prepare_crawler(selected["novels"][0]["url"])
            return await self.get_novel_info(update, context)

        # Create buttons for source selection
        buttons = []
        for i, novel in enumerate(selected["novels"]):
            hostname = urlparse(novel["url"]).hostname or novel["url"]
            info = novel.get("info", "").replace("\n", " ").strip()
            button_text = f"{i + 1}. {hostname[:30]}{'...' if len(hostname) > 30 else ''}"
            if info:
                button_text += f" - {info[:20]}{'...' if len(info) > 20 else ''}"
            buttons.append([button_text])

        title = selected["title"].replace("\n", " ").strip()
        await update.message.reply_markdown(
            f'Choose a source to download *"{title}"*, or send /cancel to stop this session.',
            reply_markup=ReplyKeyboardMarkup(buttons, one_time_keyboard=True),
        )
        return "handle_select_source"

    async def handle_select_source(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        selected = session.get("selected")
        if not selected:
            await update.message.reply_text("Selection data lost. Please start again.")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()

        text = update.message.text.strip()
        source = None
        if text.isdigit():
            idx = int(text) - 1
            if 0 <= idx < len(selected["novels"]):
                source = selected["novels"][idx]
        else:
            # Try to match the button text
            for i, item in enumerate(selected["novels"]):
                hostname = urlparse(item["url"]).hostname or item["url"]
                button_text = f"{i + 1}. {hostname[:30]}{'...' if len(hostname) > 30 else ''}"
                if text.startswith(button_text):
                    source = item
                    break

        if not source:
            await update.message.reply_text(
                "❌ Please select a valid source from the list."
            )
            return await self.show_source_selection(update, context)

        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)
            
        app.crawler = prepare_crawler(source["url"])
        return await self.get_novel_info(update, context)

    async def get_novel_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        # Validate session
        if not session or "app" not in session:
            await update.message.reply_text("No active session. Please use /start first.")
            return ConversationHandler.END
        
        app = session["app"]
        
        # Update last activity time
        session["last_activity"] = time.time()
        
        await update.message.reply_text(f"🌐 Novel URL: {app.crawler.novel_url}")
        await update.message.reply_text("📖 Reading novel info...")
        
        # Run the blocking function in a non-blocking thread with timeout
        try:
            await asyncio.wait_for(
                asyncio.to_thread(app.get_novel_info),
                timeout=180.0  # 180 seconds timeout
            )
        except asyncio.TimeoutError:
            logger.warning("Getting novel info timed out for chat_id %s", chat_id)
            await update.message.reply_text(
                "⏱️ Getting novel information timed out. The website might be slow or unresponsive.\n"
                "Please try again or choose a different source."
            )
            return await self.destroy_app(update, context)
        except Exception as e:
            logger.exception("Failed to get novel info for chat_id %s: %s", chat_id, e)
            await update.message.reply_text(f"❌ Failed to get novel info: {str(e)}")
            return await self.destroy_app(update, context)
        
        # Check cache
        cache_folder = f"{app.output_path}/json"
        if os.path.exists(cache_folder):
            await update.message.reply_text(
                "💾 Local cache found. Do you want to *use the cached data*?",
                reply_markup=ReplyKeyboardMarkup([["Yes", "No"]], one_time_keyboard=True),
                parse_mode=ParseMode.MARKDOWN
            )
            return "handle_delete_cache"
        else:
            try:
                await asyncio.to_thread(os.makedirs, app.output_path, exist_ok=True)
            except Exception as e:
                logger.exception("Failed to create output directory: %s", e)
                await update.message.reply_text("❌ Failed to create working directory. Please try again.")
                return await self.destroy_app(update, context)
                
            await update.message.reply_text(
                f"✅ Found *{len(app.crawler.volumes)} volumes* and *{len(app.crawler.chapters)} chapters*.",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode=ParseMode.MARKDOWN
            )
            return await self.display_range_selection_help(update)

    async def handle_delete_cache(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()

        user_response = update.message.text.strip().lower()
        
        if USER_RESPONSE_YES.startswith(user_response):
            try:
                # Use async file operations
                await asyncio.to_thread(shutil.rmtree, app.output_path, ignore_errors=True)
                await asyncio.to_thread(os.makedirs, app.output_path, exist_ok=True)
                await update.message.reply_text("✅ Cache cleared successfully.")
            except Exception as e:
                logger.exception("Failed to clear cache: %s", e)
                await update.message.reply_text("❌ Failed to clear cache. Continuing with existing data.")
        elif not USER_RESPONSE_NO.startswith(user_response):
            # Handle unexpected inputs
            await update.message.reply_text(
                "❓ Please respond with 'Yes' or 'No'.",
                reply_markup=ReplyKeyboardMarkup([["Yes", "No"]], one_time_keyboard=True)
            )
            return "handle_delete_cache"

        await update.message.reply_text(
            f"✅ Found *{len(app.crawler.volumes)} volumes* and *{len(app.crawler.chapters)} chapters*.",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=ParseMode.MARKDOWN
        )
        return await self.display_range_selection_help(update)

    async def display_range_selection_help(self, update: Update):
        if not update.message:
            return ConversationHandler.END
            
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        await update.message.reply_text(
            "📥 *Select what to download*\n\n"
            "Use these commands to choose your chapter range:\n\n"
            "• /all – Download *everything*\n"
            "• /last – Download the *last 50* chapters\n"
            "• /first – Download the *first 50* chapters\n"
            "• /volume – Choose specific *volumes* to download\n"
            "• /chapter – Choose a *chapter range* to download\n\n"
            "To terminate this session, send /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return "handle_range_selection"

    async def range_selection_done(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text("Session expired. Please start again with /start")
            return ConversationHandler.END
            
        app = session.get("app")
        if not app:
            await update.message.reply_text("Session data is corrupted. Please start again with /start")
            return await self.destroy_app(update, context)
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        await update.message.reply_text(
            f"✅ You have selected *{len(app.chapters)} chapters* to download",
            parse_mode=ParseMode.MARKDOWN
        )
        if len(app.chapters) == 0:
            await update.message.reply_text("❌ No chapters selected. Please try again.")
            return await self.display_range_selection_help(update)

        await update.message.reply_text(
            "📚 Do you want to generate a single file or split the books into volumes?",
            reply_markup=ReplyKeyboardMarkup(
                [["Single file", "Split by volumes"]], one_time_keyboard=True
            ),
        )
        return "handle_pack_by_volume"

    async def handle_range_all(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        app.chapters = app.crawler.chapters[:]
        return await self.range_selection_done(update, context)

    async def handle_range_first(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        app.chapters = app.crawler.chapters[:50]
        return await self.range_selection_done(update, context)

    async def handle_range_last(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        app.chapters = app.crawler.chapters[-50:]
        return await self.range_selection_done(update, context)

    async def handle_range_volume(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        # Create a formatted list of volumes
        volume_ids = [str(vol["id"]) for vol in app.crawler.volumes]
        volume_names = [vol.get("title", f"Volume {vol['id']}") for vol in app.crawler.volumes]
        
        await update.message.reply_text(
            f"📚 Available volumes:\n" + 
            "\n".join([f"• Volume {vol_id}: {name}" for vol_id, name in zip(volume_ids, volume_names)]) +
            "\n\nEnter volume numbers you want to download, separated by spaces or commas (e.g., '1 3 5' or '2,4,6')."
        )
        return "handle_volume_selection"

    async def handle_volume_selection(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        text = update.message.text
        # Extract all numbers from the input
        selected = re.findall(r"\d+", text)
        
        if not selected:
            await update.message.reply_text("❌ No valid volume numbers found. Please try again with numbers separated by spaces or commas.")
            return "handle_volume_selection"
            
        await update.message.reply_text(f"✅ Selected volumes: {', '.join(selected)}")
        selected_ids = [int(x) for x in selected]
        app.chapters = [
            chap for chap in app.crawler.chapters if chap["volume"] in selected_ids
        ]
        
        if not app.chapters:
            await update.message.reply_text("❌ No chapters found in the selected volumes. Please try different volumes.")
            return "handle_range_selection"
            
        return await self.range_selection_done(update, context)

    async def handle_range_chapter(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        await update.message.reply_text(
            f"📖 I found {len(app.crawler.chapters)} chapters\n"
            "Enter the start and end chapter numbers separated by space or comma (e.g., '10 20' or '5,15')."
        )
        return "handle_chapter_selection"

    async def handle_chapter_selection(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        text = update.message.text
        selected = re.findall(r"\d+", text)
        if len(selected) != 2:
            await update.message.reply_text(
                "❌ I need exactly two numbers (start and end chapter). Please try again."
            )
            return "handle_range_chapter"
            
        try:
            start_chapter = int(selected[0])
            end_chapter = int(selected[1])
        except ValueError:
            await update.message.reply_text("❌ Invalid chapter numbers. Please use numeric values only.")
            return "handle_range_chapter"
            
        total_chapters = len(app.crawler.chapters)
        if start_chapter < 1 or end_chapter > total_chapters or start_chapter > end_chapter:
            await update.message.reply_text(
                f"❌ Invalid chapter range. Please enter numbers between 1 and {total_chapters}, "
                f"with the start chapter less than or equal to the end chapter."
            )
            return "handle_range_chapter"
            
        app.chapters = app.crawler.chapters[start_chapter - 1 : end_chapter]
        await update.message.reply_text(
            f"✅ Selected chapters:\n"
            f"• Start chapter: {start_chapter}\n"
            f"• End chapter: {end_chapter}\n"
            f"• Total chapters: {len(app.chapters)}"
        )
        return await self.range_selection_done(update, context)

    async def handle_pack_by_volume(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        user_choice = update.message.text.strip().lower()
        app.pack_by_volume = "split" in user_choice.lower()
        
        await update.message.reply_text(
            "📁 I will " + ("split output files into volumes" if app.pack_by_volume else 
                            "generate single output files whenever possible")
        )

        # Create format selection buttons
        format_buttons = [["all"]] + [
            available_formats[i : i + 2] for i in range(0, len(available_formats), 2)
        ]
        await update.message.reply_text(
            "💾 In which format do you want me to generate your book?",
            reply_markup=ReplyKeyboardMarkup(format_buttons, one_time_keyboard=True),
        )
        return "handle_output_format"

    async def handle_output_format(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        chat_id = str(update.effective_message.chat_id)
        valid, session, app = await self.validate_session(update, context)
        if not valid:
            return ConversationHandler.END
            
        # Update last activity time
        session["last_activity"] = time.time()
        
        text = update.message.text.strip().lower()
        app.output_formats = {x: False for x in available_formats}
        
        if text == "all":
            app.output_formats = {x: True for x in available_formats}
            formats_text = ", ".join(available_formats)
        elif text in available_formats:
            app.output_formats[text] = True
            formats_text = text
        else:
            valid_formats = ", ".join(available_formats + ["all"])
            await update.message.reply_text(
                f"❌ Sorry, I did not understand. Try one of: {valid_formats}"
            )
            return "handle_output_format"

        # Schedule the download process as a job
        job = context.job_queue.run_once(
            self.start_download_process,
            1,
            name=chat_id,
            chat_id=chat_id,
            data={"app": app, "chat_id": chat_id},
        )
        session["job"] = job
        session["status"] = "Starting download..."
        
        await update.message.reply_markdown(
            "✅ *Download Scheduled*\n\n"
            f"I will generate your book in *{formats_text}* format(s).\n\n"
            "🕒 You can use the /status command at any time to check the progress.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    async def start_download_process(self, context: ContextTypes.DEFAULT_TYPE):
        job = context.job
        if not job or not hasattr(job, 'data'):
            logger.error("Job data missing in start_download_process")
            return
            
        chat_id = job.data["chat_id"]
        session = self.active_sessions.get(chat_id)
        if not session:
            try:
                await context.bot.send_message(chat_id, text="❌ Error: Session not found. Please start a new session with /start.")
            except Exception as e:
                logger.error(f"Failed to send session error message: {e}")
            return

        app = session.get("app")
        if not app:
            try:
                await context.bot.send_message(
                    chat_id, text="❌ Error: No app instance found. Please start a new session with /start."
                )
            except Exception as e:
                logger.error(f"Failed to send app error message: {e}")
            return await self.destroy_app(None, context, job)

        try:
            # Start the download process in a separate thread
            session["download_future"] = self.executor.submit(
                self.run_download, app, chat_id, session, context
            )
            # Schedule a polling job to check progress
            context.job_queue.run_repeating(
                self.check_download_progress,
                interval=5,
                first=5,  # Wait 5 seconds before first check
                name=f"{chat_id}_progress",
                chat_id=chat_id,
                data={"app": app, "chat_id": chat_id},
            )
            
            # Send initial download message
            try:
                await context.bot.send_message(
                    chat_id,
                    text=f"🚀 Starting download of *{app.crawler.novel_title}*...",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.warning(f"Failed to send download start message: {e}")
                
        except Exception as e:
            logger.exception("Failed to start download process for chat_id %s: %s", chat_id, e)
            session["error"] = f"Failed to start download: {str(e)}"
            try:
                await context.bot.send_message(chat_id, text=f"❌ {session['error']}")
            except Exception as send_error:
                logger.error(f"Failed to send error message: {send_error}")

    def run_download(self, app, chat_id, session, context):
        try:
            session["status"] = f'Downloading "{app.crawler.novel_title}"'
            for progress in app.start_download():
                # Update progress
                session["progress"] = app.progress
                time.sleep(0.1)  # Small delay to avoid overwhelming the session
            
            session["status"] = "✅ Download finished"
            session["download_future"] = None
            
            # Run binding in a separate thread to avoid blocking
            session["bind_future"] = self.executor.submit(
                self.run_bind_books, app, chat_id, session, context
            )
        except Exception as e:
            logger.exception("Failed to download for chat_id %s: %s", chat_id, e)
            session["error"] = f"❌ Download failed: {str(e)}"
            session["status"] = session["error"]

    def run_bind_books(self, app, chat_id, session, context):
        try:
            session["status"] = "📚 Generating output files"
            for progress in app.bind_books():
                # Update progress
                time.sleep(0.1)  # Small delay
            
            session["status"] = "✅ Output files generated"
            session["bind_future"] = None
            session["upload_pending"] = True
        except Exception as e:
            logger.exception("Failed to bind books for chat_id %s: %s", chat_id, e)
            session["error"] = f"❌ Failed to generate books: {str(e)}"
            session["status"] = session["error"]

    async def check_download_progress(self, context: ContextTypes.DEFAULT_TYPE):
        job = context.job
        if not job:
            return
            
        chat_id = job.data["chat_id"]
        session = self.active_sessions.get(chat_id)
        
        # Session no longer exists
        if not session:
            job.schedule_removal()
            return

        app = session.get("app")
        
        # Error occurred during process
        if session.get("error"):
            try:
                await context.bot.send_message(chat_id, text=session["error"])
            except Exception as e:
                logger.error(f"Failed to send error message: {e}")
            job.schedule_removal()
            return await self.destroy_app(None, context, job)

        # Check if download or binding is still in progress
        download_future = session.get("download_future")
        bind_future = session.get("bind_future")
        
        if download_future and download_future.running():
            progress = getattr(app, 'progress', 0)
            chapters = len(getattr(app, 'chapters', []))
            status = session.get("status", "Processing...")
            
            try:
                await context.bot.send_message(
                    chat_id,
                    text=f"📥 *{status}*\n"
                         f"Progress: *{int(progress)}%*\n"
                         f"Chapters: *{int(progress * chapters / 100)}/{chapters}*",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.warning(f"Failed to send progress update: {e}")
            return
            
        # Check if binding is complete and ready for upload
        if session.get("upload_pending"):
            if not hasattr(app, "archived_outputs") or not app.archived_outputs:
                try:
                    await context.bot.send_message(
                        chat_id, text="❌ No output files were generated."
                    )
                except Exception as e:
                    logger.error(f"Failed to send no files message: {e}")
                job.schedule_removal()
                return await self.destroy_app(None, context, job)

            # Send each file to the user
            for archive in app.archived_outputs:
                try:
                    file_size = os.stat(archive).st_size
                    file_name = os.path.basename(archive)
                    
                    if file_size < MAX_FILE_SIZE:
                        await context.bot.send_chat_action(chat_id, "upload_document")
                        await context.bot.send_document(
                            chat_id,
                            document=open(archive, "rb"),
                            filename=file_name,
                            caption=f"✅ Here's your book: *{file_name}*",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    else:
                        await context.bot.send_message(
                            chat_id,
                            text=f"☁️ *{file_name}* is too large to send directly ({file_size / 1024 / 1024:.1f} MB).\n"
                                 "Uploading to cloud storage...",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        description = f"Generated by Lightnovel Crawler Telegram Bot for user {chat_id}"
                        direct_link = upload(archive, description)
                        await context.bot.send_message(
                            chat_id, 
                            text=f"✅ Get your file here:\n{direct_link}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                except Exception as e:
                    logger.error(
                        "Failed to send file %s for chat_id %s: %s",
                        archive,
                        chat_id,
                        e,
                    )
                    try:
                        await context.bot.send_message(
                            chat_id,
                            text=f"❌ Failed to send file {os.path.basename(archive)}: {str(e)}"
                        )
                    except Exception as send_error:
                        logger.error(f"Failed to send file error message: {send_error}")

            session["upload_pending"] = False
            job.schedule_removal()
            
            # Send completion message
            try:
                await context.bot.send_message(
                    chat_id,
                    text="🎉 All files have been delivered!\n\n"
                         "Your session will be closed automatically. "
                         "Use /start to begin a new session.",
                    reply_markup=ReplyKeyboardRemove()
                )
            except Exception as e:
                logger.error(f"Failed to send completion message: {e}")
                
            await self.destroy_app(None, context, job)
            return
            
        # Binding in progress
        if bind_future and bind_future.running():
            try:
                await context.bot.send_message(
                    chat_id,
                    text=f"📚 *{session.get('status', 'Processing...')}*",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.warning(f"Failed to send binding status: {e}")
            return

    async def handle_downloader(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not update.effective_message:
            return
            
        chat_id = str(update.effective_message.chat_id)
        session = self.active_sessions.get(chat_id)
        
        if not session:
            await update.message.reply_text(
                "ℹ️ No active session. Please start a new session with /start."
            )
            return

        app = session.get("app")
        status = session.get("status", "Processing...")
        
        # Update last activity time
        session["last_activity"] = time.time()
        
        if app and hasattr(app, "chapters") and len(app.chapters) > 0:
            progress = getattr(app, 'progress', 0)
            total_chapters = len(app.chapters)
            downloaded = int(progress * total_chapters / 100)
            
            await update.message.reply_text(
                f"📊 *{status}*\n"
                f"Progress: *{int(progress)}%*\n"
                f"Chapters: *{downloaded}/{total_chapters}*\n\n"
                "Send /cancel to stop this session.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                f"⏳ *{status}*\n"
                "No chapters selected yet. Please continue with your selection or send /cancel.",
                parse_mode=ParseMode.MARKDOWN
            )