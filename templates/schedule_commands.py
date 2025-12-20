"""
Schedule Management System for Discord Bot
Handles team schedules, daily reminders, and pre-event notifications

Usage:
1. Import in main bot file: from schedule_commands import setup_schedule
2. Call setup_schedule(bot) in on_ready event
"""

from discord.ext import commands, tasks
from discord.ui import Button, View, Modal, TextInput
import discord
import json
import os
from datetime import datetime, timedelta, time
from collections import defaultdict
import asyncio

# Configuration
SCHEDULE_FILE = 'schedule.json'
DAILY_REMINDER_TIME = '09:00'  # Format: HH:MM (24-hour)
PRE_EVENT_REMINDER_MINUTES = 10

# Global schedule data
# Format: {guild_id: {channel_id: int, events: [{day, time, description, notified: bool}]}}
schedule_data = {}
bot_instance = None


def load_schedule():
    """Load schedules from JSON file"""
    global schedule_data
    
    if not os.path.exists(SCHEDULE_FILE):
        schedule_data = {}
        return
    
    try:
        with open(SCHEDULE_FILE, 'r') as f:
            schedule_data = json.load(f)
            # Convert string keys back to integers
            schedule_data = {int(k): v for k, v in schedule_data.items()}
        print(f"Loaded schedules for {len(schedule_data)} servers")
    except Exception as e:
        print(f"Error loading schedule: {e}")
        schedule_data = {}


def save_schedule():
    """Save schedules to JSON file"""
    try:
        with open(SCHEDULE_FILE, 'w') as f:
            json.dump(schedule_data, f, indent=2)
    except Exception as e:
        print(f"Error saving schedule: {e}")


def parse_schedule_input(text):
    """
    Parse schedule input text into structured events
    
    Expected format:
    monday 20:00 scrim sk
    tuesday 16:00 scrim zeta, 18:00 mc
    wednesday 19:00 review vods
    
    Returns: List of {day, time, description} dicts
    """
    events = []
    lines = text.strip().split('\n')
    
    days_map = {
        'monday': 0, 'mon': 0,
        'tuesday': 1, 'tue': 1, 'tues': 1,
        'wednesday': 2, 'wed': 2,
        'thursday': 3, 'thu': 3, 'thur': 3, 'thurs': 3,
        'friday': 4, 'fri': 4,
        'saturday': 5, 'sat': 5,
        'sunday': 6, 'sun': 6
    }
    
    current_day = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        parts = line.lower().split()
        if not parts:
            continue
        
        # Check if line starts with a day
        if parts[0] in days_map:
            current_day = parts[0].capitalize()
            line_remainder = ' '.join(parts[1:])
        else:
            line_remainder = line
        
        if not current_day:
            continue
        
        # Split by comma for multiple events on same day
        event_parts = line_remainder.split(',')
        
        for event_part in event_parts:
            event_part = event_part.strip()
            
            # Extract time (HH:MM format)
            time_found = None
            description = event_part
            
            import re
            time_match = re.search(r'\b(\d{1,2}):(\d{2})\b', event_part)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
                
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    time_found = f"{hour:02d}:{minute:02d}"
                    # Remove time from description
                    description = event_part.replace(time_match.group(0), '').strip()
            
            if time_found and description:
                events.append({
                    'day': current_day,
                    'time': time_found,
                    'description': description,
                    'notified': False
                })
    
    # Sort events by day and time
    day_order = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
                 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
    
    events.sort(key=lambda x: (day_order.get(x['day'], 7), x['time']))
    
    return events


def create_schedule_embed(guild_id):
    """Create an embed displaying the weekly schedule"""
    if guild_id not in schedule_data or not schedule_data[guild_id].get('events'):
        embed = discord.Embed(
            title="📅 Weekly Schedule",
            description="No events scheduled yet.\nUse `!schedule` to add events.",
            color=discord.Color.red()
        )
    else:
        embed = discord.Embed(
            title="📅 Weekly Schedule",
            description="Upcoming events for this week",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        events = schedule_data[guild_id]['events']
        
        # Group events by day
        days_events = defaultdict(list)
        for event in events:
            days_events[event['day']].append(event)
        
        
        
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            if day in days_events:
                event_text = []
                for event in days_events[day]:
                    # Use larger bold text with better spacing
                    event_text.append(f"**`{event['time']}`** • {event['description']}")
                
                embed.add_field(
                    name=f"\n__{day}__",  # Underline day name
                    value="\n".join(event_text) + "\n\u200b",  # Add invisible character for spacing
                    inline=False
                )
    
    # Add reminder settings if available (AFTER the if/else block)
    if guild_id in schedule_data:
        daily_time = schedule_data[guild_id].get('daily_reminder_time', DAILY_REMINDER_TIME)
        pre_minutes = schedule_data[guild_id].get('pre_event_minutes', PRE_EVENT_REMINDER_MINUTES)
        
        embed.add_field(
            name="\n⚙️ Reminder Settings",
            value=f"Daily reminder: **{daily_time}**\nPre event reminder: **{pre_minutes} min before**",
            inline=False
        )
    
    return embed
def get_today_events(guild_id):
    """Get events happening today"""
    if guild_id not in schedule_data:
        return []
    
    today = datetime.now().strftime('%A')  # e.g., 'Monday'
    events = schedule_data[guild_id].get('events', [])
    
    today_events = [e for e in events if e['day'] == today]
    return sorted(today_events, key=lambda x: x['time'])


def get_next_event(guild_id):
    """Get the next upcoming event"""
    if guild_id not in schedule_data:
        return None
    
    now = datetime.now()
    current_day = now.strftime('%A')
    current_time = now.strftime('%H:%M')
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    current_day_index = day_order.index(current_day)
    
    events = schedule_data[guild_id].get('events', [])
    
    # First, check events later today
    today_events = [e for e in events if e['day'] == current_day and e['time'] > current_time]
    if today_events:
        return min(today_events, key=lambda x: x['time'])
    
    # Then check upcoming days this week
    for i in range(1, 8):
        check_day_index = (current_day_index + i) % 7
        check_day = day_order[check_day_index]
        day_events = [e for e in events if e['day'] == check_day]
        if day_events:
            return min(day_events, key=lambda x: x['time'])
    
    return None


def get_event_datetime(event):
    """Convert event (day + time) to next occurring datetime"""
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    now = datetime.now()
    current_day_index = now.weekday()  # 0 = Monday
    target_day_index = day_order.index(event['day'])
    
    days_ahead = target_day_index - current_day_index
    if days_ahead < 0:  # Target day already happened this week
        days_ahead += 7
    elif days_ahead == 0:  # Same day - check if time has passed
        event_time = datetime.strptime(event['time'], '%H:%M').time()
        if now.time() > event_time:
            days_ahead = 7
    
    target_date = now + timedelta(days=days_ahead)
    event_time = datetime.strptime(event['time'], '%H:%M').time()
    
    return datetime.combine(target_date.date(), event_time)


async def send_daily_reminder(guild_id):
    """Send daily schedule reminder at configured time"""
    if guild_id not in schedule_data:
        return
    
    channel_id = schedule_data[guild_id].get('reminder_channel_id')  # Changed from 'channel_id'
    if not channel_id:
        return
    
    channel = bot_instance.get_channel(channel_id)
    if not channel:
        return
    
    today_events = get_today_events(guild_id)
    
    if not today_events:
        return
    
    embed = discord.Embed(
        title="Today's Schedule:",
        description=f"**{datetime.now().strftime('%A, %B %d')}**",
        color=discord.Color.red(),
        timestamp=datetime.now()
    )
    
    event_text = []
    for event in today_events:
        event_text.append(f"**{event['time']}** - {event['description']}")
    
    embed.add_field(
        name="Upcoming Events",
        value="\n".join(event_text),
        inline=False
    )
    
    await channel.send(embed=embed)
    print(f"Sent daily reminder to guild {guild_id}")


async def send_pre_event_reminder(guild_id, event, minutes=10):
    """Send reminder X minutes before an event"""
    if guild_id not in schedule_data:
        return
    
    channel_id = schedule_data[guild_id].get('reminder_channel_id')  # Changed from 'channel_id'
    if not channel_id:
        return
    
    channel = bot_instance.get_channel(channel_id)
    if not channel:
        return
    
    embed = discord.Embed(
        title="Event Starting Soon!",
        description=f"**{event['description']}** starts in {minutes} minutes!",
        color=discord.Color.red(),
        timestamp=datetime.now()
    )
    
    embed.add_field(name="Time", value=f"**{event['time']}**", inline=True)
    
    
    await channel.send("@here", embed=embed)
    print(f"Sent {minutes}-minute reminder for '{event['description']}' to guild {guild_id}")
async def send_pre_event_reminder(guild_id, event, minutes=10):
    """Send reminder X minutes before an event"""
    if guild_id not in schedule_data:
        return
    
    channel_id = schedule_data[guild_id].get('channel_id')
    if not channel_id:
        return
    
    channel = bot_instance.get_channel(channel_id)
    if not channel:
        return
    
    embed = discord.Embed(
        title="Event Starting Soon!",
        description=f"**{event['description']}** starts in {minutes} minutes!",
        color=discord.Color.red(),
        timestamp=datetime.now()
    )
    
    embed.add_field(name="Time", value=f"**{event['time']}**", inline=True)
    
    
    await channel.send("@here", embed=embed)
    print(f"Sent {minutes}-minute reminder for '{event['description']}' to guild {guild_id}")
@tasks.loop(minutes=1)
async def check_schedule_reminders():
    """Check for events that need reminders"""
    now = datetime.now()
    current_time_str = now.strftime('%H:%M')
    current_day = now.strftime('%A')
    
    for guild_id in list(schedule_data.keys()):
        guild_settings = schedule_data[guild_id]
        
        # Get per-server settings (with defaults)
        daily_time = guild_settings.get('daily_reminder_time', DAILY_REMINDER_TIME)
        pre_minutes = guild_settings.get('pre_event_minutes', PRE_EVENT_REMINDER_MINUTES)
        
        # Check daily reminders
        if current_time_str == daily_time:
            try:
                await send_daily_reminder(guild_id)
            except Exception as e:
                print(f"Error sending daily reminder to guild {guild_id}: {e}")
    
    # Check pre-event reminders
    for guild_id in list(schedule_data.keys()):
        guild_settings = schedule_data[guild_id]
        pre_minutes = guild_settings.get('pre_event_minutes', PRE_EVENT_REMINDER_MINUTES)
        events = guild_settings.get('events', [])
        
        for event in events:
            if event['day'] != current_day:
                continue
            
            # Calculate event time
            event_time = datetime.strptime(event['time'], '%H:%M').time()
            event_datetime = datetime.combine(now.date(), event_time)
            
            # Check if event is in exactly pre_minutes
            time_until_event = (event_datetime - now).total_seconds() / 60
            
            if 0 < time_until_event <= pre_minutes + 1 and not event.get('notified'):
                try:
                    await send_pre_event_reminder(guild_id, event, pre_minutes)
                    event['notified'] = True
                    save_schedule()
                except Exception as e:
                    print(f"Error sending pre-event reminder: {e}")
    
    # Reset notified flags at midnight
    if current_time_str == '00:00':
        for guild_id in schedule_data:
            for event in schedule_data[guild_id].get('events', []):
                event['notified'] = False
        save_schedule()

class ChannelSelectView(View):
    """View for selecting schedule and reminder channels"""
    
    def __init__(self, guild_id):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.schedule_channel_id = None
        self.reminder_channel_id = None
        
        # Add schedule channel select
        schedule_select = discord.ui.ChannelSelect(
            placeholder="1. Select channel to POST schedule",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            row=0
        )
        schedule_select.callback = self.schedule_channel_callback  # ADD THIS LINE
        self.add_item(schedule_select)
        
        # Add reminder channel select
        reminder_select = discord.ui.ChannelSelect(
            placeholder="2. Select channel for REMINDERS",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            row=1
        )
        reminder_select.callback = self.reminder_channel_callback  # ADD THIS LINE
        self.add_item(reminder_select)
        
        # Add continue button (disabled initially)
        self.continue_btn = Button(
            label="Continue to Schedule Input",
            style=discord.ButtonStyle.primary,
            disabled=True,
            row=2
        )
        self.continue_btn.callback = self.continue_callback
        self.add_item(self.continue_btn)
    
    async def schedule_channel_callback(self, interaction: discord.Interaction):
        self.schedule_channel_id = int(interaction.data['values'][0])
        
        # Enable continue button if both channels selected
        if self.schedule_channel_id and self.reminder_channel_id:
            self.continue_btn.disabled = False
            await interaction.response.edit_message(view=self)
        else:
            await interaction.response.defer()
    
    async def reminder_channel_callback(self, interaction: discord.Interaction):
        self.reminder_channel_id = int(interaction.data['values'][0])
        
        # Enable continue button if both channels selected
        if self.schedule_channel_id and self.reminder_channel_id:
            self.continue_btn.disabled = False
            await interaction.response.edit_message(view=self)
        else:
            await interaction.response.defer()
    
    async def continue_callback(self, interaction: discord.Interaction):
        # Show schedule input modal
        modal = ScheduleModal(self.guild_id, self.schedule_channel_id, self.reminder_channel_id)
        await interaction.response.send_modal(modal)

class ScheduleModal(Modal, title="Set Weekly Schedule"):
    """Modal for inputting schedule"""
    
    schedule_input = TextInput(
        label="Enter your weekly schedule",
        style=discord.TextStyle.paragraph,
        placeholder="monday 20:00 scrim sk\ntuesday 16:00 scrim zeta, 18:00 mc\nwednesday 19:00 review vods",
        required=True,
        max_length=2000
    )
    
    daily_reminder_time = TextInput(
        label="Daily reminder time (HH:MM in your timezone)",
        style=discord.TextStyle.short,
        placeholder="09:00",
        required=True,
        default="09:00",
        max_length=5
    )
    
    pre_event_minutes = TextInput(
        label="Remind how many minutes before events?",
        style=discord.TextStyle.short,
        placeholder="10",
        required=True,
        default="10",
        max_length=3
    )
    
    def __init__(self, guild_id, schedule_channel_id, reminder_channel_id):
        super().__init__()
        self.guild_id = guild_id
        self.schedule_channel_id = schedule_channel_id
        self.reminder_channel_id = reminder_channel_id
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        # Validate daily reminder time
        try:
            datetime.strptime(self.daily_reminder_time.value, '%H:%M')
            daily_time = self.daily_reminder_time.value
        except:
            await interaction.followup.send(
                "❌ Invalid daily reminder time format. Use HH:MM (e.g., 09:00)",
                ephemeral=True
            )
            return
        
        # Validate pre-event minutes
        try:
            pre_minutes = int(self.pre_event_minutes.value)
            if pre_minutes < 1 or pre_minutes > 120:
                raise ValueError
        except:
            await interaction.followup.send(
                "❌ Pre-event reminder must be between 1-120 minutes.",
                ephemeral=True
            )
            return
        
        # Parse schedule
        events = parse_schedule_input(self.schedule_input.value)
        
        if not events:
            await interaction.followup.send(
                "❌ Could not parse any valid events. Please check your format.\n"
                "Example: `monday 20:00 scrim sk`",
                ephemeral=True
            )
            return
        
        # Save schedule with BOTH channels
        schedule_data[self.guild_id] = {
            'schedule_channel_id': self.schedule_channel_id,  # Where to post schedule
            'reminder_channel_id': self.reminder_channel_id,   # Where to send reminders
            'events': events,
            'daily_reminder_time': daily_time,
            'pre_event_minutes': pre_minutes
        }
        save_schedule()
        
        # Show confirmation
        embed = create_schedule_embed(self.guild_id)
        embed.set_footer(text=f"Schedule saved! {len(events)} events added.")
        
        await interaction.followup.send(
            f"Schedule posted to <#{self.schedule_channel_id}>\n"
            f"Reminders will be sent to <#{self.reminder_channel_id}>",
            embed=embed,
            ephemeral=True
        )
        
        # Post schedule to the SCHEDULE channel
        schedule_channel = interaction.guild.get_channel(self.schedule_channel_id)
        if schedule_channel:
            schedule_embed = create_schedule_embed(self.guild_id)
            message = await schedule_channel.send(embed=schedule_embed)
            
            # Save the message ID so we can delete it later
            schedule_data[self.guild_id]['schedule_message_id'] = message.id
            save_schedule()


def setup_schedule(bot):
    """Register schedule commands with the bot"""
    global bot_instance
    bot_instance = bot
    
    # Load existing schedules
    load_schedule()
    
    @bot.command(name='schedule')
    async def schedule_command(ctx):
        """Set up weekly schedule with reminders"""
        view = ChannelSelectView(ctx.guild.id)
        await ctx.send(
            "📅 **Schedule Setup**\n\n"
            "1) Select channel to **post** the schedule\n"
            "2) Select channel for **reminders**\n"
            "3) Click Continue and enter your schedule\n\n"
            "**Format:**\n"
            "`monday 20:00 scrim sk`\n"
            "`tuesday 16:00 scrim zeta, 18:00 mc`",
            view=view
        )
    
    
    @bot.command(name='clear')
    async def clear_schedule_command(ctx):
        """Clear the weekly schedule and delete the posted message"""
        if ctx.guild.id in schedule_data:
            # Try to delete the schedule message if we have the channel and message ID
            schedule_channel_id = schedule_data[ctx.guild.id].get('schedule_channel_id')
            schedule_message_id = schedule_data[ctx.guild.id].get('schedule_message_id')
            
            if schedule_channel_id and schedule_message_id:
                try:
                    schedule_channel = ctx.guild.get_channel(schedule_channel_id)
                    if schedule_channel:
                        message = await schedule_channel.fetch_message(schedule_message_id)
                        await message.delete()
                        await ctx.send("Schedule cleared and message deleted.")
                    else:
                        await ctx.send("Schedule cleared. (Could not find channel to delete message)")
                except discord.NotFound:
                    await ctx.send("Schedule cleared. (Message was already deleted)")
                except discord.Forbidden:
                    await ctx.send("Schedule cleared. (No permission to delete message)")
                except Exception as e:
                    print(f"Error deleting schedule message: {e}")
                    await ctx.send("Schedule cleared. (Could not delete message)")
            else:
                await ctx.send("Schedule cleared.")
            
            # Delete the schedule data
            del schedule_data[ctx.guild.id]
            save_schedule()
        else:
            await ctx.send("No schedule found to clear.")
    
    @bot.command(name='next')
    async def next_event_command(ctx):
        """Show the next upcoming event"""
        next_event = get_next_event(ctx.guild.id)
        
        if not next_event:
            await ctx.send("No upcoming events scheduled.")
            return
        
        event_dt = get_event_datetime(next_event)
        time_until = event_dt - datetime.now()
        
        hours = int(time_until.total_seconds() // 3600)
        minutes = int((time_until.total_seconds() % 3600) // 60)
        
        embed = discord.Embed(
            title="Next Event",
            color=discord.Color.red()
        )
        
        embed.add_field(name="Event", value=next_event['description'], inline=False)
        embed.add_field(name="Day", value=next_event['day'], inline=True)
        embed.add_field(name="Time", value=next_event['time'], inline=True)
        embed.add_field(name="In", value=f"{hours}h {minutes}m", inline=True)
        
        await ctx.send(embed=embed)
    
    # Start the reminder task
    if not check_schedule_reminders.is_running():
        check_schedule_reminders.start()
        print("Schedule reminder system started")
    
    print("Schedule commands registered!")