
# --- START OF FULLY REVISED FILE with MongoDB, Ball Count, DM Leaderboards & CRICKET DEBUG LOGGING (v6.1 - Corrected DB Connection) ---

import telebot
from telebot import types # For Inline Keyboards
import random
import logging
from uuid import uuid4
import os
from keep_alive import keep_alive
import html
import urllib.parse
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, ReplyParameters, LinkPreviewOptions
from pymongo import MongoClient, ReturnDocument # Import pymongo
# Import specific pymongo errors for better exception handling
from pymongo.errors import ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError
from datetime import datetime, timezone # For timestamping user registration and ping

# --- Bot Configuration ---
# BOT_TOKEN = "YOUR_BOT_TOKEN" # Replace with your bot token
BOT_TOKEN = "8127800981:AAGq4GhNNEgjFujcb4AadqRuF34Fh9LUzw4" # Replace with your bot token
if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN:
    print("ERROR: Please replace 'YOUR_BOT_TOKEN' with your actual bot token.")
    exit()

# --- MongoDB Configuration ---
# MONGO_URI = "YOUR_MONGODB_URI" # Replace with your MongoDB URI
MONGO_URI = "mongodb+srv://2005yes2005_db_user:JQgmccBhXMgcl12l@cluster0.qhcaciu.mongodb.net/?appName=Cluster0" # Replace with your MongoDB URI
MONGO_DB_NAME = "tct_cricket_bot_db"
if MONGO_URI == "YOUR_MONGODB_URI" or not MONGO_URI:
     print("ERROR: Please configure MONGO_URI.")
     # exit() # Commented out for testing without DB if needed

bot = telebot.TeleBot(BOT_TOKEN)
bot_username = None # Will be fetched later

# --- Admin Configuration ---
xmods = [6293455550, 6265981509]

# --- Database Setup ---
client = None # Initialize client to None
db = None
users_collection = None
try:
    # CORRECTED: Removed keepAlive=True
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000, # How long to wait to find a suitable server
        connectTimeoutMS=10000,       # How long to wait for initial connection
        socketTimeoutMS=15000        # How long to wait for a socket operation (read/write)
        # keepAlive=True              # <-- This line was removed
    )
    client.admin.command('ping') # Verify connection
    db = client[MONGO_DB_NAME]
    users_collection = db.users
    print("Successfully connected to MongoDB and pinged the deployment.")
except Exception as e:
    print(f"ERROR: Could not connect to MongoDB at {MONGO_URI}.")
    print(f"Error details: {e}")
    db = None # Ensure db is None if connection fails
    users_collection = None
    if client: # Close client if partially initialized
        try:
            client.close()
        except Exception as close_e:
             print(f"Error closing potentially partial MongoDB client: {close_e}")
        client = None
    print("Warning: Bot running without database persistence.")


# --- Cricket Game States ---
STATE_WAITING = "WAITING"
STATE_TOSS = "TOSS"
STATE_BAT_BOWL = "BAT_BOWL"
STATE_P1_BAT = "P1_BAT"
STATE_P1_BOWL_WAIT = "P1_BOWL_WAIT"
STATE_P2_BAT = "P2_BAT"
STATE_P2_BOWL_WAIT = "P2_BOWL_WAIT"

# --- In-memory storage for active games ---
games = {}

# --- Logging ---
# Configure logging to show INFO level messages
# Consider writing to a file in production: logging.basicConfig(level=logging.INFO, filename='bot.log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = telebot.logger
# telebot.logger.setLevel(logging.INFO) # Already set by basicConfig

# --- Helper Functions ---
def get_player_name_telebot(user):
    if user is None: return "Unknown Player"
    name = user.first_name
    if user.last_name: name += f" {user.last_name}"
    if not name and user.username: name = f"@{user.username}"
    if not name: name = f"User_{user.id}"
    # Basic HTML escape for safety in Markdown mentions later
    return html.escape(name)

def create_standard_keyboard_telebot(game_id, buttons_per_row=3):
    markup = types.InlineKeyboardMarkup(row_width=buttons_per_row)
    buttons = [types.InlineKeyboardButton(str(i), callback_data=f"num:{i}:{game_id}") for i in range(1, 7)]
    markup.add(*buttons)
    return markup

def cleanup_game_telebot(game_id, chat_id, reason="ended", edit_markup=True):
    logger.info(f"Cleaning up game {game_id} in chat {chat_id} (Reason: {reason})")
    game_data = games.pop(game_id, None)
    if game_data and game_data.get('message_id') and edit_markup:
        if reason != "finished normally": # Don't remove markup if game ended via win/loss message edit
            try:
                bot.edit_message_reply_markup(chat_id=chat_id, message_id=game_data['message_id'], reply_markup=None)
                logger.info(f"Removed reply markup for cleaned up game {game_id}")
            except Exception as e:
                # Ignore "message is not modified" and "not found" errors as they are expected in some cleanup scenarios
                if "message is not modified" not in str(e) and "message to edit not found" not in str(e):
                    logger.error(f"Could not edit reply markup for game {game_id} on cleanup: {e}")
        # else:
            # logger.info(f"Skipping markup removal for game {game_id} as it finished normally.")

# --- Database Helper Functions ---
def get_user_data(user_id_str):
    """Fetches user data. Returns user document or None."""
    if users_collection is None:
        logger.warning(f"get_user_data called for {user_id_str} but DB is unavailable.")
        return None
    try:
        # Added a timeout directly to the find_one call, though serverSelectionTimeout should handle most cases
        user_doc = users_collection.find_one({"_id": user_id_str}, max_time_ms=3000) # 3 seconds timeout
        return user_doc
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as e:
        # Catch specific connection/timeout errors
        logger.error(f"DB Connection/Timeout error fetching user {user_id_str}: {e}")
        return None # Return None to indicate failure
    except Exception as e:
        # Catch other potential DB errors
        logger.error(f"Unexpected DB error fetching user {user_id_str}: {e}")
        return None # Return None to indicate failure


def register_user(user: types.User):
    if users_collection is None: return False
    user_id_str = str(user.id); now = datetime.utcnow()
    # Use user.full_name if available (more reliable than first/last concatenation)
    full_name = html.escape(user.full_name or get_player_name_telebot(user))
    user_doc = {
        "$set": {
            "full_name": full_name,
            "username": user.username, # Store username if available
            "last_seen": now
        },
        "$setOnInsert": {
            "_id": user_id_str,
            "runs": 0,
            "wickets": 0,
            "achievements": [],
            "registered_at": now
        }
    }
    try:
        result = users_collection.update_one({"_id": user_id_str}, user_doc, upsert=True)
        return result.upserted_id is not None or result.matched_count > 0
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as e:
        logger.error(f"DB Connection/Timeout error registering user {user_id_str}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected DB error registering user {user_id_str}: {e}")
        return False

def add_runs_to_user(user_id_str, runs_to_add):
    if users_collection is None or runs_to_add <= 0: return False
    try:
        result = users_collection.update_one({"_id": user_id_str}, {"$inc": {"runs": runs_to_add}}, upsert=False)
        if result.matched_count == 0:
             logger.warning(f"Attempted to add runs to non-existent user {user_id_str}")
             return False
        return True
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as e:
        logger.error(f"DB Connection/Timeout error adding runs to user {user_id_str}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected DB error adding runs to user {user_id_str}: {e}")
        return False

def add_wicket_to_user(user_id_str):
    if users_collection is None: return False
    try:
        result = users_collection.update_one({"_id": user_id_str}, {"$inc": {"wickets": 1}}, upsert=False)
        if result.matched_count == 0:
            logger.warning(f"Attempted to add wicket to non-existent user {user_id_str}")
            return False
        return True
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as e:
        logger.error(f"DB Connection/Timeout error adding wicket to user {user_id_str}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected DB error adding wicket to user {user_id_str}: {e}")
        return False

# Helper to get mention from DB doc
def get_user_mention_from_db(user_doc):
    if not user_doc: return "Unknown User"
    uid_str = user_doc.get("_id")
    # Prioritize full_name from DB, fallback carefully
    name = user_doc.get("full_name") or f"User {uid_str}"
    # Ensure name is escaped before putting in Markdown link
    escaped_name = html.escape(name)
    return f"[{escaped_name}](tg://user?id={uid_str})"

# --- Leaderboard Display Logic (Helper Functions) ---
def _display_runs_leaderboard(chat_id):
    """Fetches and sends the Top 10 Runs Leaderboard to the specified chat."""
    if users_collection is None:
        try: bot.send_message(chat_id, "⚠️ Database connection is unavailable. Cannot fetch leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send DB error msg to {chat_id}: {send_err}")
        return
    try:
        # Added max_time_ms for safety
        top = list(users_collection.find(
            {"runs": {"$gt": 0}},
            {"_id": 1, "full_name": 1, "runs": 1}
        ).sort("runs", -1).limit(10).max_time_ms(5000)) # 5 second timeout

        if not top:
            bot.send_message(chat_id, "🏏 No runs scored by anyone yet.")
            return
        medals = ['🥇', '🥈', '🥉'] # Only top 3 get medals
        rank_markers = ['4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']
        txt = "🏆 *Top 10 Run Scorers:*\n\n"
        for i, u in enumerate(top):
            rank_prefix = medals[i] if i < len(medals) else (rank_markers[i-len(medals)] if i-len(medals) < len(rank_markers) else f"{i+1}.")
            # Use the helper function for consistent mentions
            mention = get_user_mention_from_db(u)
            txt += f"{rank_prefix} {mention} - *{u.get('runs', 0)}* runs\n"
        bot.send_message(chat_id, txt, parse_mode='Markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
        logger.error(f"DB Connection/Timeout error fetching runs leaderboard for {chat_id}: {db_e}")
        try: bot.send_message(chat_id, "⚠️ A database issue occurred while fetching the leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send leaderboard error msg to {chat_id}: {send_err}")
    except Exception as e:
        logger.error(f"Unexpected error during runs leaderboard for {chat_id}: {e}")
        try: bot.send_message(chat_id, "⚠️ An unexpected error occurred while fetching the leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send leaderboard error msg to {chat_id}: {send_err}")


def _display_wickets_leaderboard(chat_id):
    """Fetches and sends the Top 10 Wickets Leaderboard to the specified chat."""
    if users_collection is None:
        try: bot.send_message(chat_id, "⚠️ Database connection is unavailable. Cannot fetch leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send DB error msg to {chat_id}: {send_err}")
        return
    try:
        # Added max_time_ms for safety
        top = list(users_collection.find(
            {"wickets": {"$gt": 0}},
            {"_id": 1, "full_name": 1, "wickets": 1}
        ).sort("wickets", -1).limit(10).max_time_ms(5000)) # 5 second timeout

        if not top:
            bot.send_message(chat_id, "🎯 No wickets taken by anyone yet.")
            return
        medals = ['🥇', '🥈', '🥉'] # Only top 3 get medals
        rank_markers = ['4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']
        txt = "🎯 *Top 10 Wicket Takers:*\n\n"
        for i, u in enumerate(top):
            rank_prefix = medals[i] if i < len(medals) else (rank_markers[i-len(medals)] if i-len(medals) < len(rank_markers) else f"{i+1}.")
            # Use the helper function for consistent mentions
            mention = get_user_mention_from_db(u)
            txt += f"{rank_prefix} {mention} - *{u.get('wickets', 0)}* wickets\n"
        bot.send_message(chat_id, txt, parse_mode='Markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
        logger.error(f"DB Connection/Timeout error fetching wickets leaderboard for {chat_id}: {db_e}")
        try: bot.send_message(chat_id, "⚠️ A database issue occurred while fetching the leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send leaderboard error msg to {chat_id}: {send_err}")
    except Exception as e:
        logger.error(f"Unexpected error during wickets leaderboard for {chat_id}: {e}")
        try: bot.send_message(chat_id, "⚠️ An unexpected error occurred while fetching the leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send leaderboard error msg to {chat_id}: {send_err}")


# --- Command Handlers ---

@bot.message_handler(commands=['start'])
def handle_start(message: Message):
    user = message.from_user
    user_id_str = str(user.id)
    chat_id = message.chat.id
    # Use helper function for name, handles potential HTML issues
    safe_full_name = get_player_name_telebot(user)
    mention = f"[{safe_full_name}](tg://user?id={user_id_str})" # Mention using the safer name

    # --- Deep Link Handling ---
    if message.chat.type == 'private':
        args = message.text.split()
        if len(args) > 1:
            payload = args[1]
            logger.info(f"User {user.id} started bot in private with payload: {payload}")
            if payload == 'show_lead_runs':
                _display_runs_leaderboard(chat_id)
                return # Don't proceed to registration message
            elif payload == 'show_lead_wickets':
                _display_wickets_leaderboard(chat_id)
                return # Don't proceed to registration message
            # Add other potential payloads here if needed
            # else: pass through to registration if payload is unknown

    # --- Standard /start in Group ---
    if message.chat.type != 'private':
         bot.reply_to(message, "Welcome! Use /cricket in a group to play. Use /start in my DM to register for stats and view leaderboards.")
         return

    # --- Standard /start in DM (Registration) ---
    if users_collection is None:
         bot.reply_to(message, "⚠️ Database connection is currently unavailable. Registration and stats features are temporarily disabled.")
         return

    # Check if user exists and update details if they do
    user_data = get_user_data(user_id_str)
    if user_data:
        # User exists, attempt to update their name/username/last_seen
        update_success = register_user(user)
        if update_success:
             logger.info(f"User {user_id_str} already registered. Updated details.")
        else:
             logger.warning(f"User {user_id_str} already registered, but failed to update details.")
        # Inform the user they are registered
        bot.reply_to(message, f"{mention}, you are already registered! Use /help to see commands.", parse_mode='markdown')
        return

    # User doesn't exist, attempt to register
    if register_user(user):
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Channel', url='https://t.me/TCTCRICKET'),
                   InlineKeyboardButton('Group', url='https://t.me/+SIzIYQeMsRsyOWM1')) # Corrected group link
        welcome_text = (f"Welcome {mention} to the TCT OFFICIAL BOT!\n"
                        f"You are now registered.\n\n"
                        f"Use /help for commands or check leaderboards:\n"
                        f"/lead_runs\n"
                        f"/lead_wickets")
        bot.send_message(message.chat.id, welcome_text, parse_mode='markdown', reply_markup=markup,
                         link_preview_options=LinkPreviewOptions(is_disabled=True))
        logger.info(f"New user registered: {safe_full_name} ({user_id_str})")
        # Notify admin (only if registration was successful)
        try:
            if xmods:
                # Use MarkdownV2 for safer escaping in admin notification if needed, but Markdown is often fine
                admin_mention = f"[{safe_full_name}](tg://user?id={user_id_str})"
                bot.send_message(xmods[0], f"➕ New user: {admin_mention} (`{user_id_str}`)",
                                 parse_mode='markdown',
                                 link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e:
            logger.error(f"Could not notify admin about new user {user_id_str}: {e}")
    else:
        # Registration failed (likely DB error caught in register_user)
        bot.reply_to(message, "⚠️ An error occurred during registration. This might be a temporary database issue. Please try again later.")


@bot.message_handler(commands=['help'])
def help_command(message):
    is_admin = message.from_user.id in xmods
    user_commands = """*User Commands:*
  `/start` - Register (in DM) or handle deep links.
  `/help` - This help message.
  `/cricket` - Start a cricket game (in group).
  `/cancel` - Cancel your current game (in group).
  `/my_achievement` - View your stats & achievements (reply or DM).
  `/lead_runs` - View Top 10 Run Scorers (DM recommended).
  `/lead_wickets` - View Top 10 Wicket Takers (DM recommended).
  `/ping` - Check bot and database status.""" # Added ping to help
    admin_commands = """*Admin Commands:*
  `/achieve <user_id> <title>` - Add achievement (or reply).
  `/remove_achievement <user_id> <title>` - Remove achievement (or reply).
  `/broad <message>` - Broadcast (or reply).
  `/reduce_runs <user_id> <amount>` - Reduce runs (or reply).
  `/reduce_wickets <user_id> <amount>` - Reduce wickets (or reply).
  `/clear_all_stats` - Reset all stats (use with caution!).
  `/user_count` - Show total registered users."""
    help_text = "📜 *Available Commands*\n" + user_commands
    if is_admin: help_text += "\n\n" + admin_commands
    bot.reply_to(message, help_text, parse_mode='Markdown')


# --- MODIFIED /cricket with DEBUG LOGGING ---
@bot.message_handler(commands=['cricket'])
def start_cricket(message: Message):
    user = message.from_user
    user_id_str = str(user.id)
    chat_id = message.chat.id
    player1_name = get_player_name_telebot(user) # Get escaped name

    logger.info(f"User {player1_name} ({user.id}) initiated /cricket in chat {chat_id}")

    if message.chat.type == 'private':
        logger.info(f"/cricket: Command used in private chat by {user.id}. Replying.")
        bot.reply_to(message, "Cricket games can only be started in group chats.")
        return

    # --- DB Registration Check ---
    user_exists = False # Flag to track registration status
    logger.info(f"/cricket: Checking DB status for user {user_id_str}")
    if users_collection is not None:
        logger.info(f"/cricket: Attempting get_user_data for user {user_id_str}")
        try:
            # Optional: Proactive ping before the query - might add latency
            # logger.debug(f"/cricket: Pinging DB before get_user_data for {user_id_str}")
            # client.admin.command('ping') # Use client directly
            # logger.debug(f"/cricket: DB ping successful before get_user_data for {user_id_str}")

            user_doc = get_user_data(user_id_str) # Calls function with its own error handling

            if user_doc is None and client is not None:
                # get_user_data returns None on DB error OR if user not found. Need to differentiate.
                # If client exists, we assume the DB *should* be reachable, so None likely means not found or error during fetch.
                # get_user_data already logs the specific error.
                 logger.warning(f"/cricket: get_user_data returned None for {user_id_str}. Assuming user not found or DB error occurred.")
                 # Forcing registration check is safer if DB access failed here
                 return bot.reply_to(message, "⚠️ Could not verify registration status due to a database issue. Please try again.")
                 # OR just treat as unregistered:
                 # user_exists = False

            elif user_doc:
                 logger.info(f"/cricket: get_user_data SUCCESS - User {user_id_str} found.")
                 user_exists = True
            else:
                 # user_doc is None and client is None (DB was down initially) OR user genuinely not found
                 logger.info(f"/cricket: get_user_data COMPLETE - User {user_id_str} NOT found (or DB down).")
                 user_exists = False

        except Exception as e:
            # Catch any unexpected error during the check phase itself
            logger.error(f"Unexpected Error during /cricket DB check logic for {user_id_str}: {e}", exc_info=True)
            bot.reply_to(message, "⚠️ An unexpected error occurred while checking registration. Please try again later.")
            return # Stop processing the command

        # Handle unregistered user
        if not user_exists:
            logger.info(f"/cricket: User {user_id_str} determined as not registered, sending registration message.")
            # Use the potentially safer player1_name variable
            return bot.reply_to(message, f"@{player1_name}, please /start me in DM first to register before playing.")

    elif users_collection is None: # DB was unavailable from the start
        logger.warning(f"/cricket: DB unavailable, allowing game start for {user_id_str} ({player1_name}) without registration check.")
        # Allow game start even if DB is down, but log it. user_exists remains False implicitly.

    # --- Check for Existing Games ---
    logger.info(f"/cricket: Checking for existing games involving user {user.id} in chat {chat_id}. Current active games in memory: {len(games)}")
    conflicting_game_found = False
    for gid, gdata in list(games.items()): # Iterate over a copy
        if gdata.get('chat_id') == chat_id: # Safe check for chat_id
            p1_id = gdata.get('player1', {}).get('id')
            p2_id = gdata.get('player2', {}).get('id')

            # Check if user is P1 waiting
            if gdata.get('state') == STATE_WAITING and p1_id == user.id:
                logger.info(f"/cricket: User {user.id} is P1 of existing waiting game {gid}. Replying.")
                conflicting_game_found = True
                bot.reply_to(message, "You already started a game waiting for players. Use /cancel first if you want to restart.")
                return # Exit

            # Check if user is P1 or P2 of any active game in this chat
            if user.id == p1_id or (p2_id and user.id == p2_id):
                 logger.info(f"/cricket: User {user.id} already participating in active game {gid}. Replying.")
                 conflicting_game_found = True
                 bot.reply_to(message, "You are already participating in an active game in this chat! Use /cancel if you wish to stop it.")
                 return # Exit

            # Optional: Prevent multiple concurrent games in the same chat *at all*
            # if gdata.get('state') != STATE_WAITING:
            #    logger.info(f"/cricket: Another game ({gid}) is already in progress in chat {chat_id}. Replying.")
            #    conflicting_game_found = True
            #    bot.reply_to(message, "Another game is already in progress in this chat.")
            #    return # Exit

    # Log after loop completion if no conflict caused an early return
    if not conflicting_game_found:
        logger.info(f"/cricket: Finished checking games. No conflicting game found for user {user.id}.")

    # --- Create Game ---
    logger.info(f"/cricket: Proceeding to create game object for user {user.id} ({player1_name}) in chat {chat_id}")
    game_id = str(uuid4())
    game_data = {
        'game_type': 'cricket', # Add game type for potential future expansion
        'chat_id': chat_id,
        'message_id': None,
        'state': STATE_WAITING,
        'player1': {'id': user.id, 'name': player1_name, 'user_obj': user}, # Store escaped name
        'player2': None,
        'p1_score': 0, 'p2_score': 0, 'innings': 1,
        'current_batter': None, 'current_bowler': None, 'toss_winner': None,
        'p1_toss_choice': None, 'batter_choice': None, 'target': None,
        'ball_count': 0
    }
    games[game_id] = game_data
    logger.info(f"/cricket: Game object {game_id} created and added to memory.")

    # --- Send 'Join Game' Message ---
    markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Join Game 🏏", callback_data=f"join:_:{game_id}"))
    logger.info(f"/cricket: Attempting to send 'Join Game' message for game {game_id} in chat {chat_id}.")
    try:
        # Use the escaped player1_name
        sent_message = bot.send_message(
            chat_id,
            f"🏏 New Cricket Game started by {player1_name}!\n\nWaiting for a second player to join...",
            reply_markup=markup,
            parse_mode="Markdown" # Markdown for name linking
        )
        games[game_id]["message_id"] = sent_message.message_id
        logger.info(f"/cricket: Successfully sent 'Join Game' message {sent_message.message_id} for game {game_id}.")
    except Exception as e:
        logger.error(f"Failed to send initial cricket game message {game_id} for chat {chat_id}: {e}", exc_info=True)
        # Clean up the game entry if sending the initial message fails
        games.pop(game_id, None)
        logger.info(f"/cricket: Removed game object {game_id} from memory due to send failure.")
        # Optionally notify user
        try:
             bot.reply_to(message, "⚠️ Oops! Could not start the game message. Please try again.")
        except Exception as reply_err:
             logger.error(f"Failed to send game start error reply: {reply_err}")


# --- End of MODIFIED /cricket ---


@bot.message_handler(commands=['cancel'])
def cancel_cricket(message):
    user = message.from_user; chat_id = message.chat.id; game_to_cancel_id = None
    safe_user_name = get_player_name_telebot(user) # Use helper

    if message.chat.type == 'private':
        bot.reply_to(message, "You can only cancel games in the group chat where they were started.")
        return

    logger.info(f"User {safe_user_name} ({user.id}) trying to /cancel in chat {chat_id}")
    game_found = False
    for gid, gdata in list(games.items()): # Iterate over a copy
        if gdata.get('chat_id') == chat_id:
             p1_id = gdata.get('player1', {}).get('id')
             p2_id = gdata.get('player2', {}).get('id')
             # Allow cancellation if the user is P1 or P2
             if user.id == p1_id or (p2_id and user.id == p2_id):
                 game_to_cancel_id = gid
                 game_found = True
                 logger.info(f"Identified game {gid} to be cancelled by user {user.id}")
                 break # Found the game to cancel

    if game_to_cancel_id:
        game_data = games.get(game_to_cancel_id) # Get data before cleanup
        # Notify players in the game message if possible
        player_text = ""
        if game_data:
            p1n = game_data.get('player1', {}).get('name', 'P1') # Name should be safe already
            p2n = game_data.get('player2', {}).get('name')
            player_text = f" ({p1n}{' vs ' + p2n if p2n else ''})"

        # Cleanup game data from memory *first*
        # Pass edit_markup=True so cleanup attempts to remove buttons from the message
        cleanup_game_telebot(game_to_cancel_id, chat_id, reason="cancelled by user", edit_markup=True)

        try:
            # Try to edit the original message to show cancellation *after* cleanup attempt
            if game_data and game_data.get('message_id'):
                logger.info(f"Attempting to edit message {game_data['message_id']} for cancelled game {game_to_cancel_id}")
                bot.edit_message_text(f"❌ Cricket game{player_text} cancelled by {safe_user_name}.",
                                      chat_id=chat_id, message_id=game_data['message_id'],
                                      reply_markup=None, # Ensure markup is removed here too
                                      parse_mode="Markdown")
                logger.info(f"Successfully edited message for cancelled game {game_to_cancel_id}")
            else:
                 # Fallback reply if editing isn't possible (no message_id or edit failed silently in cleanup)
                 logger.info(f"Could not find message_id or edit failed for cancelled game {game_to_cancel_id}, sending reply instead.")
                 bot.reply_to(message, f"❌ Cricket game{player_text} cancelled.")
        except Exception as e:
            # Ignore common errors like "message not modified" or "message to edit not found" (might happen if cleanup already edited)
            if "message is not modified" not in str(e) and "message to edit not found" not in str(e):
                 logger.warning(f"Could not edit cancel message for game {game_to_cancel_id}: {e}")
                 # Send fallback reply only if editing fails for other reasons
                 bot.reply_to(message, f"❌ Cricket game{player_text} cancelled.")
            else:
                 logger.info(f"Edit failed for cancelled game {game_to_cancel_id} (likely already edited/deleted): {e}")


    else:
        logger.info(f"User {user.id} tried to cancel but no active game participation found in chat {chat_id}.")
        bot.reply_to(message, "You aren't currently participating in an active game in this chat.")


# --- Broadcast Command (Admin) --- (Unchanged logic, added DB error check)
@bot.message_handler(commands=['broad'])
def handle_broadcast(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")

    user_ids_to_broadcast = []
    try:
        # Fetch only the _id field
        user_ids_cursor = users_collection.find({}, {"_id": 1}, max_time_ms=10000) # 10s timeout
        user_ids_to_broadcast = [user["_id"] for user in user_ids_cursor]
    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
        logger.error(f"DB Connection/Timeout error fetching users for broadcast: {db_e}")
        return bot.reply_to(message, "⚠️ Error fetching users due to database issue.")
    except Exception as e:
        logger.error(f"Unexpected DB error fetching users for broadcast: {e}")
        return bot.reply_to(message, "⚠️ Unexpected error fetching users.")

    if not user_ids_to_broadcast: return bot.reply_to(message, "⚠️ No registered users found.")

    content_to_send = None; is_forward = False
    if message.reply_to_message:
        content_to_send = message.reply_to_message
        is_forward = True
        logger.info(f"Admin {message.from_user.id} broadcasting via forward.")
    else:
        args = message.text.split(maxsplit=1)
        if len(args) < 2: return bot.reply_to(message, "⚠️ Usage: `/broadcast <message>` or reply.")
        content_to_send = args[1]
        is_forward = False
        logger.info(f"Admin {message.from_user.id} broadcasting text.")

    sent_count = 0; failed_count = 0; total_users = len(user_ids_to_broadcast)
    status_message = None
    try:
        status_message = bot.reply_to(message, f"📢 Broadcasting to {total_users} users... [0/{total_users}]")
    except Exception as e:
        logger.error(f"Failed to send initial broadcast status message: {e}")
        # Proceed without status updates if initial message fails
        status_message = None

    last_edit_time = datetime.now()

    for i, user_id_str in enumerate(user_ids_to_broadcast):
        try:
            if is_forward:
                bot.forward_message(chat_id=user_id_str, from_chat_id=message.chat.id, message_id=content_to_send.message_id)
            else:
                # Use Markdown for text broadcasts if specified, otherwise default
                bot.send_message(user_id_str, content_to_send, parse_mode="Markdown")
            sent_count += 1
        except Exception as e:
            failed_count += 1
            logger.warning(f"Broadcast failed for user {user_id_str}: {e}") # Log specific user failure

        # Update status message periodically or at the end
        now = datetime.now()
        should_edit = (status_message and
                       ((now - last_edit_time).total_seconds() > 3 or # Edit every 3 seconds
                        (i + 1) % 50 == 0 or                     # Or every 50 users
                        (i + 1) == total_users))                 # Or on the last user

        if should_edit:
             try:
                 bot.edit_message_text(f"📢 Broadcasting... [{sent_count}/{total_users}] Sent, [{failed_count}] Failed",
                                       chat_id=status_message.chat.id, message_id=status_message.message_id)
                 last_edit_time = now
             except Exception:
                 # Ignore edit errors (e.g., message too old, rate limits)
                 # Status update is best-effort
                 pass

    # Final status update (if possible) or reply
    final_text = f"📢 Broadcast Finished!\n✅ Sent: {sent_count}\n❌ Failed: {failed_count}"
    if status_message:
        try:
            bot.edit_message_text(final_text, chat_id=status_message.chat.id, message_id=status_message.message_id)
        except Exception:
            # Fallback to reply if final edit fails
            try:
                 bot.reply_to(message, final_text)
            except Exception as final_reply_e:
                 logger.error(f"Failed to send final broadcast status update: {final_reply_e}")
    else:
        # If initial status failed, send final status as a new message
        try:
             bot.reply_to(message, final_text)
        except Exception as final_reply_e:
             logger.error(f"Failed to send final broadcast status update (no initial msg): {final_reply_e}")

# --- Achievement Commands --- (Added basic DB error checks, unchanged logic)
@bot.message_handler(commands=['achieve'])
def add_achievement(message):
    if message.from_user.id not in xmods: return bot.reply_to(message,"❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")

    args = message.text.split(maxsplit=1); target_user_id_str = None; title = None

    if message.reply_to_message:
        target_user_id_str = str(message.reply_to_message.from_user.id)
        title = args[1].strip() if len(args) >= 2 else None
    else:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3: return bot.reply_to(message, "⚠️ Usage: `/achieve <user_id> <title>` or reply to a user and use `/achieve <title>`.")
        target_user_id_str = parts[1]
        if not target_user_id_str.isdigit(): return bot.reply_to(message, "⚠️ Invalid User ID format.")
        title = parts[2].strip()

    if not title: return bot.reply_to(message, "⚠️ Achievement title cannot be empty.")
    if len(title) > 100: return bot.reply_to(message, "⚠️ Achievement title too long (max 100 chars).") # Added length limit

    # Quote title for safe use in callback data
    try:
        encoded_title = urllib.parse.quote(title, safe='') # Ensure full encoding
    except Exception as e:
         logger.error(f"Failed to URL encode achievement title '{title}': {e}")
         return bot.reply_to(message, "⚠️ Error processing achievement title.")

    markup = InlineKeyboardMarkup().add(
        InlineKeyboardButton("✅ Confirm Add", callback_data=f"ach_confirm_add_{target_user_id_str}_{encoded_title}"),
        InlineKeyboardButton("❌ Cancel", callback_data="ach_cancel")
    )
    # Escape title for display in confirmation message
    escaped_title = html.escape(title)
    bot.reply_to(message, f"🏅 Add achievement \"*{escaped_title}*\" to user `{target_user_id_str}`?", reply_markup=markup, parse_mode="markdown")

@bot.message_handler(commands=['remove_achievement'])
def remove_achievement(message):
    if message.from_user.id not in xmods: return bot.reply_to(message,"❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")

    args = message.text.split(maxsplit=1); target_user_id_str = None; title = None

    if message.reply_to_message:
        target_user_id_str = str(message.reply_to_message.from_user.id)
        title = args[1].strip() if len(args) >= 2 else None
    else:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3: return bot.reply_to(message, "⚠️ Usage: `/remove_achievement <user_id> <title>` or reply and use `/remove_achievement <title>`.")
        target_user_id_str = parts[1]
        if not target_user_id_str.isdigit(): return bot.reply_to(message, "⚠️ Invalid User ID format.")
        title = parts[2].strip()

    if not title: return bot.reply_to(message, "⚠️ Achievement title cannot be empty.")
    if len(title) > 100: return bot.reply_to(message, "⚠️ Achievement title specified is too long (max 100 chars).")

    try:
        encoded_title = urllib.parse.quote(title, safe='')
    except Exception as e:
        logger.error(f"Failed to URL encode achievement title for removal '{title}': {e}")
        return bot.reply_to(message, "⚠️ Error processing achievement title.")

    markup = InlineKeyboardMarkup().add(
        InlineKeyboardButton("✅ Confirm Remove", callback_data=f"ach_confirm_remove_{target_user_id_str}_{encoded_title}"),
        InlineKeyboardButton("❌ Cancel", callback_data="ach_cancel")
    )
    escaped_title = html.escape(title)
    bot.reply_to(message, f"🗑️ Remove achievement \"*{escaped_title}*\" from user `{target_user_id_str}`?", reply_markup=markup, parse_mode="markdown")

@bot.message_handler(commands=['my_achievement'])
def view_my_stats_and_achievements(message):
    target_user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid_str = str(target_user.id)
    safe_target_name = get_player_name_telebot(target_user) # Get escaped name

    if users_collection is None:
        logger.warning(f"my_achievement called for {uid_str} but DB unavailable.")
        return bot.reply_to(message, "⚠️ Database connection is unavailable. Cannot fetch stats.")

    user_data = get_user_data(uid_str) # Uses helper with error handling

    if user_data is None:
        # Check if client exists to differentiate between user-not-found and DB-error
        if client is None:
             reply_text = "⚠️ Database connection is unavailable. Cannot fetch stats."
        else:
             # DB is likely available, so user probably not registered
             mention = f"[{safe_target_name}](tg://user?id={uid_str})"
             reply_text = f"User {mention} is not registered. "
             reply_text += "Please tell them to /start me in DM." if message.reply_to_message else "Please /start me in DM first."
        return bot.reply_to(message, reply_text, parse_mode="markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))

    # User data fetched successfully
    runs = user_data.get("runs", 0); wickets = user_data.get("wickets", 0)
    achievements = user_data.get("achievements", [])
    # Use name from DB if available and valid, otherwise use fresh name from target_user
    name_from_db = user_data.get("full_name")
    display_name = name_from_db if name_from_db else safe_target_name
    mention = f"[{display_name}](tg://user?id={uid_str})" # Use escaped name for mention

    stats_text = f"📊 Stats for {mention}:\n  🏏 Runs: *{runs}*\n  🎯 Wickets: *{wickets}*"
    achievement_text = "\n\n🏆 *Achievements*"

    if achievements:
        achievement_text += f" ({len(achievements)}):\n"
        # Escape each achievement title before displaying
        achievement_lines = [f"  🏅 `{html.escape(str(title))}`" for title in achievements]
        # Limit displayed achievements to prevent message length errors
        max_achievements_display = 20
        if len(achievement_lines) > max_achievements_display:
             achievement_text += "\n".join(achievement_lines[:max_achievements_display])
             achievement_text += f"\n  ...and {len(achievement_lines) - max_achievements_display} more."
        else:
             achievement_text += "\n".join(achievement_lines)
    else:
        achievement_text += ":\n  *None yet.*"

    try:
        bot.reply_to(message, stats_text + achievement_text, parse_mode="markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e:
         logger.error(f"Failed to send my_achievement message for {uid_str}: {e}")
         # Handle potential message length errors
         if "message is too long" in str(e):
             try:
                  bot.reply_to(message, stats_text + "\n\n🏆 *Achievements*\n  (Too many achievements to display here.)", parse_mode="markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
             except Exception: pass # Ignore error on fallback message
         else:
             # Send generic error for other issues
             try:
                  bot.reply_to(message, "⚠️ An error occurred while displaying stats.")
             except Exception: pass


# --- Stat Modification Commands (Admin) --- (Added DB error checks, unchanged logic)
@bot.message_handler(commands=['reduce_runs'])
def reduce_runs_cmd(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")

    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    parts = message.text.split(); uid_str = None; amount = None

    try:
        if target_user:
            uid_str = str(target_user.id)
            if len(parts) < 2: raise ValueError("Missing amount")
            amount = int(parts[1])
        elif len(parts) >= 3:
            uid_str = parts[1]
            if not uid_str.isdigit(): raise ValueError("Invalid User ID")
            amount = int(parts[2])
        else:
            raise ValueError("Invalid usage")

        if amount <= 0: raise ValueError("Amount must be positive")

    except (ValueError, IndexError) as e:
        logger.warning(f"Invalid reduce_runs usage by {message.from_user.id}: {e} - Text: '{message.text}'")
        return bot.reply_to(message, "⚠️ Usage: Reply `/reduce_runs <amount>` or use `/reduce_runs <user_id> <amount>`.\nAmount must be a positive number.")

    try:
        # Use find_one_and_update to get the updated document atomically
        # Project only necessary fields. Use max_time_ms.
        user_doc = users_collection.find_one_and_update(
            {"_id": uid_str},
            [{"$set": {"runs": {"$max": [0, {"$subtract": ["$runs", amount]}]}}}], # Ensure runs don't go below 0
            projection={"runs": 1, "full_name": 1},
            return_document=ReturnDocument.AFTER, # Get the document *after* update
            max_time_ms=5000 # 5 second timeout
        )

        if user_doc:
            new_runs = user_doc.get("runs", 0)
            # Use helper function to get mention safely
            mention = get_user_mention_from_db(user_doc)
            bot.reply_to(message, f"✅ Reduced *{amount}* runs from {mention}. New total: *{new_runs}*.", parse_mode="Markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
            logger.info(f"Admin {message.from_user.id} reduced {amount} runs from user {uid_str}. New total: {new_runs}")
        else:
            # If find_one_and_update returns None, the user wasn't found
            bot.reply_to(message, f"⚠️ User with ID `{uid_str}` not found in the database.")

    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
        logger.error(f"DB Connection/Timeout error reducing runs for {uid_str}: {db_e}")
        bot.reply_to(message, "⚠️ Database error occurred while reducing runs.")
    except Exception as e:
        logger.error(f"Unexpected error reducing runs for {uid_str}: {e}", exc_info=True)
        bot.reply_to(message, "⚠️ An unexpected error occurred.")


@bot.message_handler(commands=['reduce_wickets'])
def reduce_wickets_cmd(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")

    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    parts = message.text.split(); uid_str = None; amount = None

    try:
        if target_user:
            uid_str = str(target_user.id)
            if len(parts) < 2: raise ValueError("Missing amount")
            amount = int(parts[1])
        elif len(parts) >= 3:
            uid_str = parts[1]
            if not uid_str.isdigit(): raise ValueError("Invalid User ID")
            amount = int(parts[2])
        else:
            raise ValueError("Invalid usage")

        if amount <= 0: raise ValueError("Amount must be positive")

    except (ValueError, IndexError) as e:
        logger.warning(f"Invalid reduce_wickets usage by {message.from_user.id}: {e} - Text: '{message.text}'")
        return bot.reply_to(message, "⚠️ Usage: Reply `/reduce_wickets <amount>` or use `/reduce_wickets <user_id> <amount>`.\nAmount must be a positive number.")

    try:
        user_doc = users_collection.find_one_and_update(
            {"_id": uid_str},
            [{"$set": {"wickets": {"$max": [0, {"$subtract": ["$wickets", amount]}]}}}], # Ensure wickets don't go below 0
            projection={"wickets": 1, "full_name": 1},
            return_document=ReturnDocument.AFTER,
            max_time_ms=5000
        )

        if user_doc:
            new_wickets = user_doc.get("wickets", 0)
            mention = get_user_mention_from_db(user_doc) # Use helper
            bot.reply_to(message, f"✅ Reduced *{amount}* wickets from {mention}. New total: *{new_wickets}*.", parse_mode="Markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
            logger.info(f"Admin {message.from_user.id} reduced {amount} wickets from user {uid_str}. New total: {new_wickets}")
        else:
            bot.reply_to(message, f"⚠️ User with ID `{uid_str}` not found in the database.")

    except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
        logger.error(f"DB Connection/Timeout error reducing wickets for {uid_str}: {db_e}")
        bot.reply_to(message, "⚠️ Database error occurred while reducing wickets.")
    except Exception as e:
        logger.error(f"Unexpected error reducing wickets for {uid_str}: {e}", exc_info=True)
        bot.reply_to(message, "⚠️ An unexpected error occurred.")

@bot.message_handler(commands=['clear_all_stats'])
def clear_all_stats(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")

    markup = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("⚠️ YES, CLEAR ALL STATS ⚠️", callback_data="confirm_clear_stats"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_stats")
    )
    bot.reply_to(message, "🚨 *DANGER ZONE* 🚨\nThis will reset ALL runs and wickets for ALL registered users to zero. This action CANNOT be undone.\n\nAre you absolutely sure?", reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['user_count'])
def user_count(message):
     if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
     if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
     try:
         # Add timeout for safety
         count = users_collection.count_documents({}, maxTimeMS=5000)
         bot.reply_to(message, f"👥 Registered users in database: {count}")
     except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
         logger.error(f"DB Connection/Timeout error counting users: {db_e}")
         bot.reply_to(message, "⚠️ Error counting users due to database issue.")
     except Exception as e:
         logger.error(f"Unexpected DB error counting users: {e}", exc_info=True)
         bot.reply_to(message, "⚠️ Unexpected error counting users.")

# --- Leaderboard Commands (Unchanged logic, bot username fetch resilience) ---

@bot.message_handler(commands=['lead_runs'])
def show_runs_leaderboard(message: Message):
    # Attempt to fetch bot username if not already set
    global bot_username
    if bot_username is None:
        try:
             bot_me = bot.get_me()
             bot_username = bot_me.username
             logger.info(f"Fetched bot username: @{bot_username}")
        except Exception as e:
             logger.error(f"Failed to get bot username for leaderboard link: {e}")
             # Proceed without link if fetch fails, show directly in group (less ideal)
             _display_runs_leaderboard(message.chat.id)
             return

    if message.chat.type in ['group', 'supergroup'] and bot_username:
        # Send link to DM if in group and username is known
        button_url = f"https://t.me/{bot_username}?start=show_lead_runs"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📊 View Top 10 Runs (DM)", url=button_url))
        bot.reply_to(message, "Leaderboards are best viewed privately. Click the button below!", reply_markup=markup)
    else:
        # Show directly in DM or if in group and username couldn't be fetched
        _display_runs_leaderboard(message.chat.id)

@bot.message_handler(commands=['lead_wickets'])
def show_wickets_leaderboard(message: Message):
    global bot_username
    if bot_username is None:
        try:
             bot_me = bot.get_me()
             bot_username = bot_me.username
             logger.info(f"Fetched bot username: @{bot_username}")
        except Exception as e:
             logger.error(f"Failed to get bot username for leaderboard link: {e}")
             _display_wickets_leaderboard(message.chat.id)
             return

    if message.chat.type in ['group', 'supergroup'] and bot_username:
        button_url = f"https://t.me/{bot_username}?start=show_lead_wickets"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🎯 View Top 10 Wickets (DM)", url=button_url))
        bot.reply_to(message, "Leaderboards are best viewed privately. Click the button below!", reply_markup=markup)
    else:
        _display_wickets_leaderboard(message.chat.id)


# --- Central Callback Query Handler ---
@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    user = call.from_user; chat_id = call.message.chat.id
    message_id = call.message.message_id; data = call.data
    # Log callback data sanitizing potential sensitive info if needed in future
    # logger.debug(f"Callback received: User={user.id}, Chat={chat_id}, Msg={message_id}, Data='{data[:50]}...'") # Log only start of data
    logger.info(f"Callback received: User={user.id}, Chat={chat_id}, Msg={message_id}, Data='{data}'") # Log full data for now

    # --- Achievement & Stat Clear Callbacks ---
    if data.startswith("ach_") or data == "confirm_clear_stats" or data == "cancel_clear_stats":
        # Check DB connection first for these admin actions
        if users_collection is None:
             try: bot.answer_callback_query(call.id, "Database unavailable. Cannot perform action.", show_alert=True)
             except Exception as e: logger.warning(f"Failed to answer callback for DB unavailable: {e}")
             return

        if data.startswith("ach_"): # Achievement callbacks
            # Check admin privileges *before* parsing/unquoting
            if user.id not in xmods:
                 return bot.answer_callback_query(call.id, "❌ Not authorized for this action.")

            parts = data.split("_", 4)
            if parts[1] == "cancel":
                 try:
                     bot.edit_message_text("❌ Operation cancelled.", chat_id, message_id, reply_markup=None)
                 except Exception as e:
                     # Ignore common edit errors
                     if "message is not modified" not in str(e): logger.warning(f"Failed to edit 'ach_cancel' message: {e}")
                 return bot.answer_callback_query(call.id) # Answer silently

            # Need at least ach_confirm_add/remove_userid_title (5 parts)
            if len(parts) < 5:
                 logger.error(f"Invalid achievement callback format: {data}")
                 return bot.answer_callback_query(call.id, "Invalid action data.")

            # action, mode, user_id_str, encoded_title = parts[1], parts[2], parts[3], parts[4]
            mode = parts[2] # 'add' or 'remove'
            user_id_str = parts[3]
            encoded_title = parts[4]

            # Validate user ID format again just in case
            if not user_id_str.isdigit():
                 logger.error(f"Invalid User ID in achievement callback: {user_id_str}")
                 return bot.answer_callback_query(call.id, "Invalid User ID in action data.")

            # Decode the title safely
            try:
                 title = urllib.parse.unquote(encoded_title)
            except Exception as e:
                 logger.error(f"Failed to URL decode achievement title from callback '{encoded_title}': {e}")
                 return bot.answer_callback_query(call.id, "Error decoding achievement title.")

            escaped_title = html.escape(title) # Escape for display messages

            try:
                msg = "An unknown database error occurred."
                res = None
                # Check if target user exists *before* attempting update
                target_user_data = get_user_data(user_id_str)
                if target_user_data is None and client is not None:
                     # get_user_data returns None on error or not found. Assume not found if DB seems up.
                     msg = f"⚠️ User `{user_id_str}` not found in the database. Cannot modify achievements."
                     logger.warning(f"Admin {user.id} tried to modify achievements for non-existent user {user_id_str}")
                elif client is None:
                     msg = "⚠️ Database connection is unavailable." # Should have been caught earlier but double-check
                else:
                    # User exists, proceed with update
                    if mode == "add":
                        # Add achievement only if it doesn't already exist ($addToSet is idempotent)
                        res = users_collection.update_one(
                            {"_id": user_id_str},
                            {"$addToSet": {"achievements": title}},
                            max_time_ms=5000
                        )
                    elif mode == "remove":
                        # Remove achievement ($pull removes all matching instances)
                        res = users_collection.update_one(
                            {"_id": user_id_str},
                            {"$pull": {"achievements": title}},
                            max_time_ms=5000
                        )

                    # Process result
                    if res:
                        if res.matched_count == 0:
                            # Should not happen if target_user_data check passed, but handle defensively
                            msg = f"⚠️ User `{user_id_str}` somehow not found during update."
                            logger.error(f"Achievement update error: User {user_id_str} found initially but match failed.")
                        elif res.modified_count == 0:
                            # No change made - either already added or already removed/not present
                            if mode == "add":
                                msg = f"🏅 Achievement \"*{escaped_title}*\" already exists for `{user_id_str}`. No changes made."
                            else: # mode == "remove"
                                msg = f"🗑️ Achievement \"*{escaped_title}*\" was not found for `{user_id_str}`. No changes made."
                        elif mode == "add":
                            msg = f"✅ Added \"*{escaped_title}*\" achievement to `{user_id_str}`."
                            logger.info(f"Admin {user.id} added achievement '{title}' for {user_id_str}")
                        elif mode == "remove":
                            # Use markdown strikethrough for removed item
                            msg = f"🗑️ Removed \"*~~{escaped_title}~~*\" achievement from `{user_id_str}`."
                            logger.info(f"Admin {user.id} removed achievement '{title}' for {user_id_str}")
                    else:
                        # This case implies the update_one call itself failed unexpectedly before returning a result
                        msg = "⚠️ Database update operation failed unexpectedly."
                        logger.error(f"Achievement update_one call for user {user_id_str} returned None or failed.")

                # Edit the original confirmation message
                bot.edit_message_text(msg, chat_id, message_id, parse_mode="markdown", reply_markup=None)

            except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
                logger.error(f"DB Connection/Timeout error processing achievement callback {data}: {db_e}")
                try: bot.edit_message_text("⚠️ A database connection error occurred.", chat_id, message_id)
                except Exception as edit_e: logger.error(f"Failed to edit message on DB error: {edit_e}")
            except Exception as e:
                logger.error(f"Unexpected error processing achievement callback {data}: {e}", exc_info=True)
                try: bot.edit_message_text("⚠️ An unexpected error occurred.", chat_id, message_id)
                except Exception as edit_e: logger.error(f"Failed to edit message on unexpected error: {edit_e}")

            return bot.answer_callback_query(call.id) # Answer silently after handling


        elif data == "confirm_clear_stats": # Stat Clear Confirm
            if user.id not in xmods: return bot.answer_callback_query(call.id, "❌ Not authorized.")
            try:
                logger.warning(f"Admin {user.id} ({user.full_name}) CONFIRMED clear all stats.")
                # Add timeout
                res = users_collection.update_many({}, {"$set": {"runs": 0, "wickets": 0}}, max_time_ms=15000) # 15s timeout
                msg = f"🧹 Stats cleared for *{res.modified_count}* users!"
                bot.edit_message_text(msg, chat_id, message_id, reply_markup=None, parse_mode="Markdown")
                logger.warning(f"STATS CLEARED: {res.modified_count} users affected by admin {user.id}.")
                return bot.answer_callback_query(call.id, "✅ All Stats Cleared!")
            except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
                logger.error(f"DB Connection/Timeout error clearing all stats: {db_e}")
                bot.edit_message_text("⚠️ Error clearing stats due to a database issue.", chat_id, message_id)
                return bot.answer_callback_query(call.id, "DB error.")
            except Exception as e:
                logger.error(f"Unexpected error clearing all stats: {e}", exc_info=True)
                bot.edit_message_text("⚠️ An unexpected error occurred while clearing stats.", chat_id, message_id)
                return bot.answer_callback_query(call.id, "Error.")

        elif data == "cancel_clear_stats": # Stat Clear Cancel
             try:
                 bot.edit_message_text("❌ Stat clearing operation cancelled.", chat_id, message_id, reply_markup=None)
             except Exception as e:
                 if "message is not modified" not in str(e): logger.warning(f"Failed to edit 'cancel_clear_stats' message: {e}")
             return bot.answer_callback_query(call.id) # Answer silently
    # --- End Achievement/Stat Clear ---


    # --- Cricket Game Callbacks ---
    try:
        # Use more robust splitting, handle potential missing parts
        parts = data.split(":", 2)
        if len(parts) != 3:
            # logger.debug(f"Ignoring callback with unexpected format: {data}")
            return bot.answer_callback_query(call.id) # Ignore non-game format silently

        action, value_str, game_id = parts
        # Try converting value to int if it looks like a number, otherwise keep as string
        try:
            value = int(value_str) if value_str.isdigit() else value_str
        except ValueError:
            value = value_str # Keep as string if conversion fails

        # logger.debug(f"Parsed game callback: Action='{action}', Value='{value}' ({type(value)}), GameID='{game_id}'")

    except Exception as parse_err:
        logger.error(f"Error parsing callback data '{data}': {parse_err}")
        return bot.answer_callback_query(call.id) # Ignore if parsing fails


    if game_id not in games:
        logger.warning(f"Callback received for non-existent or ended game {game_id}. Data: {data}");
        try:
            # Try to edit the message to indicate the game is over
            bot.edit_message_text("This game session has ended or was cancelled.", chat_id, message_id, reply_markup=None)
        except Exception as e:
             # Ignore common errors if editing fails on an old message
             if "message is not modified" not in str(e) and "message to edit not found" not in str(e):
                 logger.warning(f"Failed to edit ended game message {message_id}: {e}")
        return bot.answer_callback_query(call.id, "Game session ended.")

    # --- Game Logic Starts ---
    game = games[game_id]
    game_type = game.get('game_type', 'cricket') # Default to cricket if type not set

    # Ensure it's a cricket game callback if mixing game types in future
    if game_type != 'cricket':
        logger.warning(f"Callback {data} received for game {game_id}, but it's not a cricket game (type: {game_type}). Ignoring.")
        return bot.answer_callback_query(call.id)

    # Ensure the callback is for the *current* game message
    if message_id != game.get("message_id"):
         logger.warning(f"Callback ignored: Stale message ID for game {game_id}. Callback MsgID {message_id} vs Game MsgID {game.get('message_id')}. Data: {data}");
         # Politely inform user on the specific callback they clicked
         return bot.answer_callback_query(call.id, "Please use the buttons on the latest game message.")

    # --- Game State Machine ---
    current_state = game.get('state'); p1 = game['player1']; p2 = game.get('player2')
    # Get player names safely (should already be escaped)
    p1_name = p1.get('name', 'Player 1'); p2_name = p2.get('name', 'Player 2') if p2 else "Player 2"
    p1_id = p1['id']; p2_id = p2['id'] if p2 else None

    # Add a log at the start of processing a valid callback for the game
    logger.info(f"Processing callback '{action}' for game {game_id}, state '{current_state}', user {user.id}")

    try:
        # --- JOIN ---
        if action == "join" and current_state == STATE_WAITING:
             if user.id == p1_id:
                 return bot.answer_callback_query(call.id, "You cannot join your own game.")
             if p2: # Already have a player 2
                 # Check if the current user is already player 2 (e.g., double click)
                 if user.id == p2_id:
                      return bot.answer_callback_query(call.id, "You have already joined.")
                 else:
                      return bot.answer_callback_query(call.id, f"{p2_name} has already joined.")

             # Check if P2 is registered (using the safer check logic)
             user_id_str = str(user.id)
             p2_registered = False
             if users_collection is not None:
                  logger.info(f"Game {game_id}: Checking registration for P2 joiner {user_id_str}")
                  p2_user_data = get_user_data(user_id_str)
                  if p2_user_data is None and client is not None:
                       # Error or not found
                       logger.warning(f"Game {game_id}: DB error or P2 joiner {user_id_str} not found.")
                       bot.answer_callback_query(call.id, "⚠️ Error checking registration status.", show_alert=True)
                       # Don't let join if status unknown/error
                       return
                  elif p2_user_data:
                       logger.info(f"Game {game_id}: P2 joiner {user_id_str} is registered.")
                       p2_registered = True
                  else:
                       # Not found or DB down
                       logger.info(f"Game {game_id}: P2 joiner {user_id_str} not registered (or DB down).")
                       p2_registered = False

             elif users_collection is None:
                  logger.warning(f"Game {game_id}: P2 joiner {user_id_str} attempting join while DB is down. Allowing.")
                  p2_registered = True # Allow join if DB is down

             if not p2_registered:
                 bot.answer_callback_query(call.id) # Answer first
                 safe_joiner_name = get_player_name_telebot(user)
                 bot.send_message(chat_id,
                                  f"@{safe_joiner_name}, please /start me in DM first to register before joining a game.",
                                  reply_parameters=ReplyParameters(message_id=message_id))
                 return # Stop processing join

             # P2 is registered or DB is down, proceed with join
             bot.answer_callback_query(call.id) # Answer query *before* editing message
             player2_name = get_player_name_telebot(user) # Get safe name
             game['player2'] = {"id": user.id, "name": player2_name, "user_obj": user}
             p2_name = player2_name # Update local variable too
             game['state'] = STATE_TOSS
             logger.info(f"Player 2 ({player2_name} - {user.id}) joined game {game_id}.")

             markup = types.InlineKeyboardMarkup(row_width=2).add(
                 types.InlineKeyboardButton("Heads", callback_data=f"toss:H:{game_id}"),
                 types.InlineKeyboardButton("Tails", callback_data=f"toss:T:{game_id}")
             )
             try:
                 # Use safe names
                 bot.edit_message_text(f"✅ {p2_name} has joined the game!\n\n"
                                       f"*{p1_name}* vs *{p2_name}*\n\n"
                                       f"*Coin Toss Time!*\n\n"
                                       f"➡️ {p1_name}, call Heads or Tails:",
                                       chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
                 logger.info(f"Game {game_id}: Message updated for Toss.")
             except Exception as e:
                 logger.error(f"Failed to edit message after P2 join G{game_id}: {e}")
                 # Attempt to inform players if edit fails
                 try: bot.send_message(chat_id, f"Error updating game message. {p1_name}, please call the toss using the buttons if they appeared.")
                 except Exception: pass


        # --- TOSS ---
        elif action == "toss" and current_state == STATE_TOSS:
             if user.id != p1_id:
                 # Mention player who needs to act
                 return bot.answer_callback_query(call.id, f"Waiting for {p1_name} to call the toss.")
             if not p2 or not p2_id: # Should not happen if state is TOSS, but check anyway
                 logger.error(f"Game {game_id}: Player 2 missing during TOSS state. Cleaning up.")
                 cleanup_game_telebot(game_id, chat_id, reason="internal error - p2 missing")
                 try: bot.edit_message_text("Error: Player 2 seems to have left. Game cancelled.", chat_id, message_id, reply_markup=None)
                 except Exception: pass
                 return bot.answer_callback_query(call.id, "Error: Player 2 missing.")

             bot.answer_callback_query(call.id) # Answer first
             choice = value # 'H' or 'T'
             coin_flip = random.choice(['H', 'T'])
             coin_result_text = 'Heads' if coin_flip == 'H' else 'Tails'
             winner_player = p1 if choice == coin_flip else p2
             loser_player = p2 if choice == coin_flip else p1
             winner_name = winner_player['name'] # Safe names

             game['toss_winner'] = winner_player['id']
             game['state'] = STATE_BAT_BOWL
             logger.info(f"Game {game_id}: P1 ({p1_name}) chose {choice}, Coin was {coin_result_text}. Toss Winner: {winner_name} ({winner_player['id']})")

             markup = types.InlineKeyboardMarkup(row_width=2).add(
                 types.InlineKeyboardButton("Bat 🏏", callback_data=f"batorbowl:bat:{game_id}"),
                 types.InlineKeyboardButton("Bowl 🧤", callback_data=f"batorbowl:bowl:{game_id}")
             )
             try:
                 bot.edit_message_text(f"Coin shows: *{coin_result_text}*.\n\n"
                                       f"🎉 *{winner_name}* won the toss!\n\n"
                                       f"➡️ {winner_name}, choose whether to Bat first or Bowl first:",
                                       chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
                 logger.info(f"Game {game_id}: Message updated for Bat/Bowl choice.")
             except Exception as e:
                 logger.error(f"Failed to edit message after toss G{game_id}: {e}")


        # --- BAT/BOWL ---
        elif action == "batorbowl" and current_state == STATE_BAT_BOWL:
             toss_winner_id = game.get('toss_winner')
             if not toss_winner_id: # Should not happen
                 logger.error(f"Game {game_id}: toss_winner missing in BAT_BOWL state. Cleaning up.")
                 cleanup_game_telebot(game_id, chat_id, reason="internal error - toss winner missing")
                 try: bot.edit_message_text("Error: Game state corrupted (toss winner missing). Game cancelled.", chat_id, message_id, reply_markup=None)
                 except Exception: pass
                 return bot.answer_callback_query(call.id, "Error: Game state issue.")

             if user.id != toss_winner_id:
                 winner_player = p1 if toss_winner_id == p1_id else p2
                 return bot.answer_callback_query(call.id, f"Waiting for {winner_player['name']} (toss winner) to choose.")
             if not p2 or not p2_id: # Double check P2 still present
                 logger.error(f"Game {game_id}: Player 2 missing during BAT_BOWL state. Cleaning up.")
                 cleanup_game_telebot(game_id, chat_id, reason="internal error - p2 missing")
                 try: bot.edit_message_text("Error: Player 2 seems to have left. Game cancelled.", chat_id, message_id, reply_markup=None)
                 except Exception: pass
                 return bot.answer_callback_query(call.id, "Error: Player 2 missing.")

             bot.answer_callback_query(call.id) # Answer first
             choice = value # 'bat' or 'bowl'
             toss_winner_player = p1 if toss_winner_id == p1_id else p2
             toss_loser_player = p2 if toss_winner_id == p1_id else p1

             batter_player = toss_winner_player if choice == "bat" else toss_loser_player
             bowler_player = toss_loser_player if choice == "bat" else toss_winner_player
             batter_name = batter_player['name'] # Safe names
             bowler_name = bowler_player['name']

             game.update({
                 'current_batter': batter_player['id'],
                 'current_bowler': bowler_player['id'],
                 'innings': 1,
                 'state': STATE_P1_BAT if batter_player['id'] == p1_id else STATE_P2_BAT, # Set state based on who bats first
                 'p1_score': 0,
                 'p2_score': 0,
                 'target': None,
                 'ball_count': 0 # Reset ball count for Innings 1
             })
             logger.info(f"Game {game_id}: {toss_winner_player['name']} chose to {choice}. {batter_name} ({batter_player['id']}) will bat first. State -> {game['state']}")

             markup = create_standard_keyboard_telebot(game_id)
             try:
                 bot.edit_message_text(f"Alright! {toss_winner_player['name']} chose to *{choice}* first.\n\n"
                                       f"*--- Innings 1 ---*\n"
                                       f"Target: To Be Determined\n\n"
                                       f"🏏 Batting: *{batter_name}*\n"
                                       f"🧤 Bowling: *{bowler_name}*\n"
                                       f"Score: 0 (Balls: 0)\n\n"
                                       f"➡️ {batter_name}, select your shot (1-6):",
                                       chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
                 logger.info(f"Game {game_id}: Message updated, Innings 1 started.")
             except Exception as e:
                 logger.error(f"Failed to edit message after bat/bowl choice G{game_id}: {e}")


        # --- Number Choice (Game Turn) ---
        elif action == "num":
            # Ensure value is an integer between 1 and 6
            if not isinstance(value, int) or not (1 <= value <= 6):
                 logger.warning(f"Game {game_id}: Invalid number choice '{value}' by user {user.id}. Ignoring.")
                 return bot.answer_callback_query(call.id, "Invalid choice. Please select 1-6.")

            # Determine expected states based on who is *currently* batting
            batter_id = game['current_batter']
            expected_batter_state = STATE_P1_BAT if batter_id == p1_id else STATE_P2_BAT
            # Bowler wait state depends on who batted (P1 bats -> P1 waits for bowl | P2 bats -> P2 waits for bowl)
            expected_bowler_state = STATE_P1_BOWL_WAIT if batter_id == p1_id else STATE_P2_BOWL_WAIT

            number_chosen = value # The number (1-6) chosen by the user

            # Critical check: P2 must exist for the game to proceed beyond waiting/setup
            if not p2 or not p2_id:
                logger.error(f"Game {game_id}: Player 2 missing during number input state ({current_state}). Cleaning up.")
                cleanup_game_telebot(game_id, chat_id, reason="internal error - p2 missing")
                try: bot.edit_message_text("Error: Your opponent seems to have left the game. Game cancelled.", chat_id, message_id, reply_markup=None)
                except Exception: pass
                return bot.answer_callback_query(call.id, "Error: Opponent missing.")

            # Identify current batter and bowler from game data
            bowler_id = game['current_bowler']
            try: # Defensive check to ensure players exist in game data
                 batter_player = p1 if batter_id == p1_id else p2
                 bowler_player = p1 if bowler_id == p1_id else p2
                 batter_name = batter_player['name'] # Safe names
                 bowler_name = bowler_player['name']
            except KeyError:
                  logger.error(f"Game {game_id}: Player data missing for batter/bowler IDs ({batter_id}/{bowler_id}). Cleaning up.")
                  cleanup_game_telebot(game_id, chat_id, reason="internal error - player data missing")
                  try: bot.edit_message_text("Error: Critical game data missing. Game cancelled.", chat_id, message_id, reply_markup=None)
                  except Exception: pass
                  return bot.answer_callback_query(call.id, "Error: Game data issue.")

            batter_id_str = str(batter_id); bowler_id_str = str(bowler_id) # For DB updates
            current_ball_count = game.get('ball_count', 0) # Get current ball count before potential increment

            # --- Batter's Turn ---
            if current_state == expected_batter_state:
                if user.id != batter_id:
                    return bot.answer_callback_query(call.id, f"It's {batter_name}'s turn to bat.")
                # Check if batter already chose (e.g., double click)
                if game.get('batter_choice') is not None:
                    return bot.answer_callback_query(call.id, "Waiting for the bowler to bowl.")

                bot.answer_callback_query(call.id, f"You played {number_chosen}. Waiting for the bowler...")
                game['batter_choice'] = number_chosen
                game['state'] = expected_bowler_state # Transition state *after* storing choice
                logger.info(f"Game {game_id}: Batter {batter_name} chose {number_chosen}. State -> {expected_bowler_state}")

                # Determine score to display (batter's current score)
                current_game_score = game['p1_score'] if batter_id == p1_id else game['p2_score']
                target = game.get('target')
                target_text = f" | Target: *{target}*" if target is not None else ""
                innings_text = f"*--- Innings {game['innings']} ---*{target_text}\n"
                markup = create_standard_keyboard_telebot(game_id) # Keyboard for bowler

                # Update message to prompt bowler, hiding batter's choice
                text = (f"{innings_text}\n"
                        f"🏏 Bat: {batter_name} (Played)\n" # Hide number
                        f"🧤 Bowl: {bowler_name}\n\n"
                        f"Score: {current_game_score} (Balls: {current_ball_count})\n\n" # Show balls *before* increment
                        f"➡️ {bowler_name}, select your delivery (1-6):")
                try:
                    bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
                    logger.info(f"Game {game_id}: Message updated for bowler's turn.")
                except Exception as e:
                     logger.error(f"Failed to edit message for bowler's turn G{game_id}: {e}")


            # --- Bowler's Turn ---
            elif current_state == expected_bowler_state:
                 if user.id != bowler_id:
                     return bot.answer_callback_query(call.id, f"It's {bowler_name}'s turn to bowl.")

                 bat_number = game.get('batter_choice')
                 # Safety check: Batter's choice MUST exist here
                 if bat_number is None:
                    logger.error(f"Game {game_id}: CRITICAL - batter_choice is None in state {current_state}. Reverting state.")
                    # Revert state to let batter choose again
                    game['state'] = expected_batter_state
                    game['batter_choice'] = None # Ensure it's cleared
                    try:
                        # Inform players of the error and prompt batter again
                        markup = create_standard_keyboard_telebot(game_id)
                        current_game_score = game['p1_score'] if batter_id == p1_id else game['p2_score']
                        target = game.get('target')
                        target_text = f" | Target: *{target}*" if target is not None else ""
                        text = (f"⚠️ Error: Batter's choice was lost. Please try again.\n\n"
                                f"*--- Innings {game['innings']} ---*{target_text}\n\n"
                                f"🏏 Batting: *{batter_name}*\n"
                                f"🧤 Bowling: *{bowler_name}*\n\n"
                                f"Score: {current_game_score} (Balls: {current_ball_count})\n\n"
                                f"➡️ {batter_name}, select your shot (1-6):")
                        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
                    except Exception as edit_err:
                        logger.error(f"Failed to edit message on batter choice error G{game_id}: {edit_err}")
                    return bot.answer_callback_query(call.id, "Error: Batter's choice missing. Batter, please choose again.")

                 # Batter choice exists, proceed with bowler's action
                 bot.answer_callback_query(call.id) # Answer bowler's query
                 bowl_number = number_chosen # The bowler's chosen number (1-6)

                 # --- Process the Ball ---
                 game['ball_count'] += 1 # <<< INCREMENT BALL COUNT HERE >>>
                 current_ball_count = game['ball_count'] # Get updated count for display/logging
                 logger.info(f"Game {game_id}: Ball {current_ball_count}, Inn {game['innings']}. Batter chose {bat_number}, Bowler chose {bowl_number}")

                 result_text = f"*{batter_name}* played: `{bat_number}`\n*{bowler_name}* bowled: `{bowl_number}`\n\n"
                 final_message_text = ""; final_markup = None; game_ended = False

                 # -- OUT --
                 if bat_number == bowl_number:
                    result_text += f"💥 *OUT!* {batter_name} is dismissed!\n"
                    logger.info(f"Game {game_id}: OUT! Batter={batter_name}({batter_id}), Bowler={bowler_name}({bowler_id}), Score={game['p1_score'] if batter_id == p1_id else game['p2_score']} @ {current_ball_count} balls.")

                    # Update bowler's wickets stat (best effort)
                    if add_wicket_to_user(bowler_id_str):
                        logger.info(f"DB: Wicket added successfully for user {bowler_id_str}")
                    # else: # add_wicket_to_user already logs errors/warnings

                    # Check if Innings 1 or 2
                    if game['innings'] == 1:
                        # End of Innings 1
                        current_game_score = game['p1_score'] if batter_id == p1_id else game['p2_score'] # Score *before* out
                        game['target'] = current_game_score + 1
                        logger.info(f"Game {game_id}: End of Innings 1. Target set to {game['target']}")
                        result_text += f"\n*End of Innings 1*. Target for {bowler_name} is *{game['target']}* runs.\n\n"
                        result_text += f"*--- Innings 2 ---*\n"

                        # Swap roles, reset ball count, reset batter choice, change state
                        new_batter_id = bowler_id
                        new_bowler_id = batter_id
                        game.update({
                            'current_batter': new_batter_id,
                            'current_bowler': new_bowler_id,
                            'innings': 2,
                            'batter_choice': None,       # Clear choice for next innings
                            'state': STATE_P1_BAT if new_batter_id == p1_id else STATE_P2_BAT, # Set state based on new batter
                            'ball_count': 0              # Reset ball count for Innings 2
                        })

                        # Update local vars for the new roles
                        new_batter_pl = bowler_player; new_bowler_pl = batter_player
                        new_batter_name = new_batter_pl['name']; new_bowler_name = new_bowler_pl['name']
                        # Get score for the *new* batter (which is 0 at start of innings 2)
                        new_batter_game_score = game['p1_score'] if new_batter_pl['id'] == p1_id else game['p2_score']

                        result_text += (f"Target: *{game['target']}*\n\n"
                                       f"🏏 Batting: *{new_batter_name}*\n"
                                       f"🧤 Bowling: *{new_bowler_name}*\n\n"
                                       f"Score: {new_batter_game_score} (Balls: 0)\n\n" # Show 0 balls
                                       f"➡️ {new_batter_name}, select your shot (1-6):")
                        final_message_text = result_text
                        final_markup = create_standard_keyboard_telebot(game_id) # Keyboard for the new batter
                        logger.info(f"Game {game_id}: Prepared for Innings 2 start. State -> {game['state']}")

                    else: # Out in Innings 2 -> Game Over
                        game_ended = True
                        bat_score = game['p1_score'] if batter_id == p1_id else game['p2_score'] # Score *before* out
                        target = game['target']
                        p1_final = game['p1_score']; p2_final = game['p2_score'] # Final scores
                        logger.info(f"Game {game_id}: Game Over! Out in Innings 2. Final Scores P1:{p1_final}, P2:{p2_final}. Target:{target}")

                        result_text += f"\n*Game Over!*\n\n--- *Final Scores* ---\n"
                        # Use safe names in final score display
                        result_text += f"👤 {game['player1']['name']}: *{p1_final}*\n"
                        result_text += f"👤 {game['player2']['name']}: *{p2_final}*\n\n"

                        # Determine winner (Bowler wins if score is less than target-1, Tie if equal to target-1)
                        if target is None: # Should not happen in Innings 2 if logic is correct
                             logger.error(f"Game {game_id}: Game Over in Innings 2 but Target is None!")
                             result_text += "Error: Target information missing."
                        elif bat_score < target - 1:
                            margin = target - 1 - bat_score
                            result_text += f"🏆 *{bowler_name} wins by {margin} runs!*"
                            logger.info(f"Game {game_id}: Winner {bowler_name} by {margin} runs.")
                        elif bat_score == target - 1:
                             result_text += f"🤝 *It's a Tie!* Scores are level."
                             logger.info(f"Game {game_id}: Result is a Tie.")
                        else:
                             # This case should ideally not happen if logic is right (score cannot be >= target if out)
                             result_text += f"Logic Error: Score ({bat_score}) >= Target ({target}) on final dismissal?"
                             logger.error(f"Game {game_id}: Win condition logic error on Inn 2 dismissal. Score={bat_score}, Target={target}")

                        final_message_text = result_text
                        final_markup = None # No more buttons

                 # -- RUNS --
                 else:
                    runs_scored = bat_number # Runs scored = batter's chosen number
                    result_text += f"🏏 Scored *{runs_scored}* runs!\n"
                    logger.info(f"Game {game_id}: Runs! Scored={runs_scored}. Batter={batter_name}({batter_id}).")

                    # Update batter's score in the game
                    current_game_score = 0
                    if batter_id == p1_id:
                        game['p1_score'] += runs_scored
                        current_game_score = game['p1_score']
                    else: # batter_id == p2_id
                        game['p2_score'] += runs_scored
                        current_game_score = game['p2_score']

                    # Update batter's total runs stat in DB (best effort)
                    if add_runs_to_user(batter_id_str, runs_scored):
                        logger.info(f"DB: Added {runs_scored} runs successfully for user {batter_id_str}")
                    # else: # add_runs_to_user already logs errors/warnings

                    # Reset batter's choice for the next ball *after* processing current ball
                    game['batter_choice'] = None

                    # Check for game end conditions (only in Innings 2)
                    target = game.get('target')
                    if game['innings'] == 2 and target is not None and current_game_score >= target:
                        # Target Chased - Game Over
                        game_ended = True
                        p1_final = game['p1_score']; p2_final = game['p2_score']
                        logger.info(f"Game {game_id}: Game Over! Target Chased in Innings 2. Final Scores P1:{p1_final}, P2:{p2_final}. Target:{target}")

                        result_text += f"\n*Target Chased! Game Over!*\n\n--- *Final Scores* ---\n"
                        # Use safe names
                        result_text += f"👤 {game['player1']['name']}: *{p1_final}*\n"
                        result_text += f"👤 {game['player2']['name']}: *{p2_final}*\n\n"
                        result_text += f"🏆 *{batter_name} wins!*" # Batter (chasing team) wins
                        logger.info(f"Game {game_id}: Winner {batter_name} (Target Chased).")

                        final_message_text = result_text
                        final_markup = None # No more buttons
                    else:
                        # Game continues, prepare for next ball (batter bats again)
                        game['state'] = expected_batter_state # Go back to batter's turn state
                        logger.info(f"Game {game_id}: Ball processed, game continues. State -> {expected_batter_state}")
                        target_text = f" | Target: *{target}*" if target is not None else ""
                        innings_text = f"*--- Innings {game['innings']} ---*{target_text}\n"

                        result_text += (f"\n{innings_text}\n"
                                        f"🏏 Batting: *{batter_name}*\n"
                                        f"🧤 Bowling: *{bowler_name}*\n\n"
                                        f"Score: {current_game_score} (Balls: {current_ball_count})\n\n" # Show updated score and ball count
                                        f"➡️ {batter_name}, select your next shot (1-6):")
                        final_message_text = result_text
                        final_markup = create_standard_keyboard_telebot(game_id) # Keyboard for batter again

                 # --- Edit Message with Result ---
                 try:
                     bot.edit_message_text(final_message_text, chat_id, message_id, reply_markup=final_markup, parse_mode="Markdown")
                     logger.info(f"Game {game_id}: Message updated with ball result/next prompt.")
                 except Exception as edit_err:
                      logger.error(f"Failed to edit message {message_id} after ball processing G{game_id}: {edit_err}")
                      # If editing fails, especially on game end, send a new message as fallback
                      if game_ended:
                           try:
                               logger.warning(f"Game {game_id}: Edit failed on game end, sending fallback message.")
                               bot.send_message(chat_id, final_message_text, parse_mode="Markdown")
                               logger.info(f"Sent game end message as fallback for G{game_id}")
                               # Clean up immediately after sending fallback if edit failed
                               cleanup_game_telebot(game_id, chat_id, reason="finished normally", edit_markup=False) # Don't try editing again
                               return # Exit callback processing for this game
                           except Exception as send_err:
                               logger.error(f"Failed to send fallback game end message for G{game_id}: {send_err}")
                      # else:
                          # If game continues and edit fails, players might get confused.
                          # Maybe send a small notification?
                          # try: bot.send_message(chat_id, "⚠️ Error updating game status. Please use buttons below if possible.")
                          # except: pass

                 # --- Cleanup if Game Ended ---
                 if game_ended:
                     logger.info(f"Game {game_id} finished normally. Cleaning up game data.")
                     # Pass edit_markup=False because we *know* we just successfully edited the message or sent a fallback
                     cleanup_game_telebot(game_id, chat_id, reason="finished normally", edit_markup=False)

        # --- Ignore other actions / invalid states ---
        else:
            logger.warning(f"Ignoring game callback action '{action}' in state '{current_state}' for game {game_id}. Data: {data}")
            bot.answer_callback_query(call.id) # Acknowledge callback even if ignored

    # --- Catch unexpected errors during game logic ---
    except Exception as e:
        # Use logger.exception to include traceback for critical errors
        logger.exception(f"!!! CRITICAL Error processing game callback for game {game_id}: Data='{data}', State='{current_state}'")
        try:
            # Inform the user and try to clean up
            bot.answer_callback_query(call.id, "An unexpected error occurred in the game logic. Game cancelled.", show_alert=True)
            # Attempt to clean up the game state if an error occurs
            cleanup_game_telebot(game_id, chat_id, reason="critical error", edit_markup=True) # Try to remove buttons
            bot.send_message(chat_id, "🚨 An unexpected error occurred with the cricket game. The game has been stopped. Please start a new one with /cricket.")
        except Exception as inner_e:
            logger.error(f"Error during critical error handling for game {game_id}: {inner_e}")

# --- Ping Command ---
@bot.message_handler(commands=['ping'])
def handle_ping(message: Message):
    """Checks bot latency and DB connection status."""
    logger.info(f"Received /ping command from {message.from_user.id} in chat {message.chat.id}")
    start_time = datetime.now(timezone.utc)
    ping_msg = None # Initialize ping_msg to None
    db_status = "N/A"
    db_ping_latency_ms = None
    total_latency_ms = None

    try:
        # 1. Send initial message
        ping_msg = bot.reply_to(message, "⏳ Pinging...")
        send_time = datetime.now(timezone.utc) # Time after sending initial message

        # 2. Check DB status
        if client is not None and users_collection is not None: # Check if client and collection seem initialized
            db_start_time = datetime.now(timezone.utc)
            try:
                # Use the client's command, more reliable than checking collection object alone
                client.admin.command('ping', maxTimeMS=3000) # 3 second timeout for DB ping
                db_end_time = datetime.now(timezone.utc)
                db_ping_latency = db_end_time - db_start_time
                db_ping_latency_ms = round(db_ping_latency.total_seconds() * 1000)
                db_status = "Connected ✅"
                logger.info(f"/ping: DB connection successful (Latency: {db_ping_latency_ms}ms)")
            except (ConnectionFailure, AutoReconnect, ServerSelectionTimeoutError, TimeoutError) as db_e:
                # Handle specific timeout or connection errors
                logger.warning(f"/ping: DB check failed (Timeout/Connection Error): {db_e}")
                db_status = f"Timeout/Error ❌"
            except Exception as db_e:
                # Handle other potential command errors
                logger.warning(f"/ping: DB check failed (Other Error): {db_e}")
                db_status = f"Error ❌"
        else:
            # If client or collection is None, initial connection likely failed.
            logger.warning(f"/ping: DB client/collection object is None. Reporting as Disconnected.")
            db_status = "Disconnected ⚠️"
            db_ping_latency_ms = None # Ensure this is None if disconnected

        # 3. Calculate bot latency (total time for the operation)
        end_time = datetime.now(timezone.utc)
        total_latency = end_time - start_time
        total_latency_ms = round(total_latency.total_seconds() * 1000)

        # 4. Format the final message
        ping_text = f"🏓 *Pong!* \n\n" \
                    f"⏱️ Bot Latency: `{total_latency_ms} ms`\n" \
                    f"🗄️ Database: `{db_status}`"
        if db_ping_latency_ms is not None:
             ping_text += f" (Ping: `{db_ping_latency_ms} ms`)"
        logger.info(f"/ping: Result - Bot Latency: {total_latency_ms}ms, DB Status: {db_status}, DB Ping: {db_ping_latency_ms}ms")

        # 5. Edit the original message
        if ping_msg: # Ensure ping_msg was successfully created
            bot.edit_message_text(ping_text, chat_id=ping_msg.chat.id, message_id=ping_msg.message_id, parse_mode="Markdown")
        else: # Fallback if initial message failed
             logger.warning("/ping: Initial reply message object was None, sending new message.")
             bot.reply_to(message, ping_text, parse_mode="Markdown")

    except Exception as e:
        # Catch errors in the ping command logic itself (e.g., sending/editing message)
        logger.error(f"Error during /ping command execution: {e}", exc_info=True)
        try:
             # Attempt to send a simple error message
             fallback_text = "⚠️ An error occurred while processing the ping command."
             if ping_msg:
                 # Try editing the original message if it exists
                 bot.edit_message_text(fallback_text, chat_id=ping_msg.chat.id, message_id=ping_msg.message_id)
             else:
                 # Otherwise, reply to the command
                 bot.reply_to(message, fallback_text)
        except Exception as fallback_e:
            logger.error(f"Error sending fallback ping error message: {fallback_e}")


# --- Start Polling ---
if __name__ == '__main__':
    logger.info("Starting Combined Cricket & Stats Bot (v6.1 - Corrected DB Connection)...")
    if users_collection is None or client is None: # Check both client and collection
        logger.warning("!!! BOT RUNNING WITHOUT DATABASE CONNECTION - STATS & REGISTRATION DISABLED !!!")
    else:
        logger.info("Database connection appears active.")

    # Fetch bot username at startup
    try:
        bot_info = bot.get_me()
        bot_username = bot_info.username
        logger.info(f"Bot username: @{bot_username} (ID: {bot_info.id})")
    except Exception as e:
        logger.critical(f"CRITICAL: Could not fetch bot username on startup: {e}. Leaderboard links might fail.")
        # bot_username remains None, handlers will show an error message or show boards directly

    try:
        try:
            keep_alive()
            logger.info("Started keep_alive webserver thread.")
        except Exception as e:
            logger.error(f"Could not start keep_alive webserver: {e}")
        logger.info("Starting bot polling...")
        # Increased timeout values slightly, may help on slow networks but unlikely to fix the core issue
        bot.infinity_polling(logger_level=logging.INFO, # Set to DEBUG for more verbose logs if needed
                             long_polling_timeout=10, # How long Telegram server waits before responding if no updates
                             timeout=20) # How long bot waits for response from Telegram server
    except Exception as poll_err:
        logger.critical(f"Bot polling loop crashed: {poll_err}", exc_info=True)
    finally:
        # Close MongoDB connection gracefully if client exists
        if client:
             try:
                 client.close()
                 logger.info("MongoDB connection closed.")
             except Exception as close_err:
                 logger.error(f"Error closing MongoDB connection: {close_err}")
        logger.info("Bot polling stopped.")

# --- END OF FULLY REVISED FILE (v6.1) ---
