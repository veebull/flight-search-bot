# Flight Search Bot

This Rust application periodically checks for available flights between specified cities using the Travelpayouts API and sends notifications via Telegram when flights are found.

## Features

- Searches for direct flights between specified cities for a date range
- Sends notifications via Telegram
- Enriches flight data with additional information from AirLabs API
- Implements rate limiting and exponential backoff
- Supports multiple notification channels

## Setup

1. Clone the repository
2. Create a `.env` file with the following variables:
   - `TRAVELPAYOUTS_API_KEY`: Your Travelpayouts API key
   - `TELEGRAM_BOT_TOKEN`: Your Telegram bot token
   - `TELEGRAM_CHAT_ID`: Your Telegram chat ID
   - `TELEGRAM_DEVLOGS_TOPIC_ID`: Topic ID for development logs
   - `TELEGRAM_FOUND_TOPIC_ID`: Topic ID for found flights
   - `AIRLABS_API_KEY`: Your AirLabs API key (optional)
   - `ORIGIN`: Origin airport code in IATA format (e.g., MOW)
   - `DESTINATION`: Destination airport code in IATA format (e.g., LED)
   - `START_DATE`: Start date for search range in ISO 8601 format (YYYY-MM-DD)
   - `END_DATE`: End date for search range in ISO 8601 format (YYYY-MM-DD)
3. Run with `cargo run`

## Configuration

Edit the origin, destination, and date range variables in `main.rs` to customize your search parameters.
