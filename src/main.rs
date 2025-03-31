use chrono::{DateTime, NaiveDate, Utc, TimeZone, NaiveDateTime, FixedOffset};
use chrono::{Datelike, Timelike};
use dotenv::dotenv;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::time::Duration;
use tokio::time;
use std::collections::HashMap;
use url::Url;
use serde_json::json;

// Updated structures for Travelpayouts API responses based on the actual JSON
#[derive(Deserialize, Debug)]
struct FlightData {
    success: bool,
    data: Option<Vec<FlightResult>>,
    currency: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize, Debug)]
struct FlightResult {
    origin: String,
    destination: String,
    origin_airport: String,
    destination_airport: String,
    price: i64,
    airline: String,
    flight_number: String,
    departure_at: String,
    return_at: Option<String>,
    transfers: i64,
    duration: Option<i64>,
    duration_to: Option<i64>,
    duration_back: Option<i64>,
    return_transfers: Option<i64>,
    link: String,
    seats: Option<i64>,
}

#[derive(Serialize)]
struct SearchParams {
    origin: String,
    destination: String,
    departure_at: String,
    return_at: Option<String>,
    currency: String,
    token: String,
}

// Add these new structures for AirLabs API
#[derive(Deserialize, Debug)]
struct AirLabsResponse {
    request: AirLabsRequest,
    response: Vec<AirLabsFlight>,
    error: Option<AirLabsError>,
}

#[derive(Deserialize, Debug)]
struct AirLabsRequest {
    lang: String,
    currency: String,
    time: i64,
    id: String,
    server: String,
    host: String,
    pid: i64,
    key: Option<String>,
    params: serde_json::Value,
    version: i64,
    method: String,
    client: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct AirLabsError {
    message: String,
    code: i64,
}

#[derive(Deserialize, Debug)]
struct AirLabsFlight {
    flight_number: String,
    airline_iata: Option<String>,
    airline_icao: Option<String>,
    dep_iata: Option<String>,
    dep_icao: Option<String>,
    arr_iata: Option<String>,
    arr_icao: Option<String>,
    dep_time: Option<String>,
    arr_time: Option<String>,
    duration: Option<i64>,
    status: Option<String>,
    aircraft_icao: Option<String>,
    reg_number: Option<String>,
    // Seat information fields - note that AirLabs may not provide exact seat availability
    seats_economy: Option<i64>,
    seats_business: Option<i64>,
    seats_first: Option<i64>,
}

// Function to convert minutes to hours and minutes format
fn format_duration(minutes: i64) -> String {
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    
    if hours > 0 {
        format!("{} ч {} мин", hours, remaining_minutes)
    } else {
        format!("{} мин", remaining_minutes)
    }
}

// Function to convert ISO datetime to human readable Russian format
fn format_datetime_ru(datetime_str: &str) -> String {
    // Parse the ISO 8601 datetime string
    if let Ok(dt) = DateTime::parse_from_rfc3339(datetime_str) {
        // Convert to local time (assuming +3 for Moscow time, adjust if needed)
        let local_time = dt.with_timezone(&FixedOffset::east_opt(3 * 3600).unwrap());
        
        // Format the date in Russian
        let day = local_time.day();
        let month = match local_time.month() {
            1 => "января",
            2 => "февраля",
            3 => "марта",
            4 => "апреля",
            5 => "мая",
            6 => "июня",
            7 => "июля",
            8 => "августа",
            9 => "сентября",
            10 => "октября",
            11 => "ноября",
            12 => "декабря",
            _ => "",
        };
        let year = local_time.year();
        let hour = local_time.hour();
        let minute = local_time.minute();
        
        format!("{} {} {} в {:02}:{:02}", day, month, year, hour, minute)
    } else {
        // Return original string if parsing fails
        datetime_str.to_string()
    }
}

// Function to get human-readable airline name
fn get_airline_name(code: &str) -> &str {
    match code {
        "UT" => "Utair",
        "SU" => "Аэрофлот",
        "S7" => "S7 Airlines",
        "U6" => "Уральские Авиалинии",
        "WZ" => "Red Wings",
        "N4" => "Nordwind",
        "DP" => "Победа",
        "R3" => "Якутия",
        "5N" => "СМАРТАВИА",
        "EO" => "Pegas Fly",
        "RT" => "ЮВТ АЭРО",
        "A4" => "Азимут",
        "IO" => "IrAero",
        "YC" => "ЯМАЛ",
        "7R" => "Руслайн",
        "KV" => "КрасАвиа",
        _ => code,
    }
}

// Function to get human-readable city name from IATA code
fn get_city_name(code: &str) -> &str {
    match code {
        "MOW" => "Москва",
        "LED" => "Санкт-Петербург",
        "UFA" => "Уфа",
        "USK" => "Усинск",
        "KZN" => "Казань",
        "AER" => "Сочи",
        "SVX" => "Екатеринбург",
        "OVB" => "Новосибирск",
        "VVO" => "Владивосток",
        "KGD" => "Калининград",
        "ROV" => "Ростов-на-Дону",
        "KRR" => "Краснодар",
        "SIP" => "Симферополь",
        "GOJ" => "Нижний Новгород",
        "SGC" => "Сургут",
        "MRV" => "Минеральные Воды",
        "CEK" => "Челябинск",
        "KUF" => "Самара",
        "BAX" => "Барнаул",
        "OMS" => "Омск",
        "TJM" => "Тюмень",
        "IKT" => "Иркутск",
        "MMK" => "Мурманск",
        "KJA" => "Красноярск",
        "VOG" => "Волгоград",
        _ => code,
    }
}

// Updated function to handle rate limiting with exponential backoff
async fn send_telegram_notification(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    message: &str,
    topic_id: &str,
    inline_keyboard: Option<serde_json::Value>,
) -> Result<(), Box<dyn Error>> {
    let api_url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    
    let mut json_body = json!({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": true
    });

     // Add message_thread_id only if topic_id is not empty and not "1"
     if !topic_id.is_empty() && topic_id != "1" {
        json_body["message_thread_id"] = json!(topic_id);
    }
    
    if let Some(keyboard) = inline_keyboard {
        json_body["reply_markup"] = keyboard;
    }
    
    // Implement exponential backoff for rate limiting
    let mut retry_count = 0;
    let max_retries = 5;
    let initial_delay = 1; // seconds
    
    loop {
    let response = client
        .post(&api_url)
        .json(&json_body)
        .send()
        .await?;
    
        if response.status().is_success() {
            // Add a small delay to avoid Telegram rate limits (30 messages per second is the limit)
            time::sleep(Duration::from_millis(1000)).await;
            return Ok(());
        } else {
        let status = response.status();
        let text = response.text().await?;
            
            // If we hit the rate limit (429 Too Many Requests)
            if status.as_u16() == 429 {
                retry_count += 1;
                
                if retry_count > max_retries {
                    return Err(format!("Exceeded maximum retries for Telegram API. Last error: {}", text).into());
                }
                
                // Extract retry_after from response if available
                let retry_after = if let Ok(error_json) = serde_json::from_str::<serde_json::Value>(&text) {
                    error_json.get("parameters")
                        .and_then(|p| p.get("retry_after"))
                        .and_then(|r| r.as_f64())
                        .unwrap_or_else(|| {
                            // Calculate exponential backoff if retry_after not provided
                            let backoff = initial_delay * 2_u64.pow(retry_count as u32);
                            backoff as f64
                        })
                } else {
                    // Fallback exponential backoff
                    let backoff = initial_delay * 2_u64.pow(retry_count as u32);
                    backoff as f64
                };
                
                let wait_time = Duration::from_secs_f64(retry_after);
                eprintln!("Telegram API rate limited (429). Waiting for {} seconds before retry {}/{}...", 
                    wait_time.as_secs(), retry_count, max_retries);
                
                time::sleep(wait_time).await;
                // Continue the loop to retry
            } else {
                // Other error, not rate limiting
        eprintln!("Telegram API request failed with status {}: {}", status, text);
                return Err(format!("Telegram API request failed: {}", text).into());
            }
        }
    }
}

// Updated function to send messages to multiple topic IDs with rate limit handling
async fn send_telegram_multi_topic_notification(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    message: &str,
    topic_ids: &[String],
    inline_keyboard: Option<serde_json::Value>,
) -> Result<(), Box<dyn Error>> {
    for topic_id in topic_ids {
        match send_telegram_notification(
            client,
            bot_token,
            chat_id,
            message,
            topic_id,
            inline_keyboard.clone()
        ).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("Error sending to topic {}: {}", topic_id, e);
                // Continue with other topics even if one fails
            }
        }
    }
    
    Ok(())
}

// Enhanced function for formatting DateTime<Utc> to Russian human-readable format
fn format_utc_datetime_ru(dt: DateTime<Utc>) -> String {
    // Convert to Moscow time (+3)
    let moscow_time = dt.with_timezone(&FixedOffset::east_opt(3 * 3600).unwrap());
    
    // Format in Russian
    let day = moscow_time.day();
    let month = match moscow_time.month() {
        1 => "января",
        2 => "февраля",
        3 => "марта",
        4 => "апреля",
        5 => "мая",
        6 => "июня",
        7 => "июля",
        8 => "августа",
        9 => "сентября",
        10 => "октября",
        11 => "ноября",
        12 => "декабря",
        _ => "",
    };
    let year = moscow_time.year();
    let hour = moscow_time.hour();
    let minute = moscow_time.minute();
    let second = moscow_time.second();
    
    format!("{} {} {} в {}ч {}м {}с", day, month, year, hour, minute, second)
}

// Function to format a date range for display
fn format_date_range_ru(start_date: &NaiveDate, end_date: &NaiveDate) -> String {
    let start_day = start_date.day();
    let start_month = match start_date.month() {
        1 => "января",
        2 => "февраля",
        3 => "марта",
        4 => "апреля",
        5 => "мая",
        6 => "июня",
        7 => "июля",
        8 => "августа",
        9 => "сентября",
        10 => "октября",
        11 => "ноября",
        12 => "декабря",
        _ => "",
    };
    let start_year = start_date.year();
    
    let end_day = end_date.day();
    let end_month = match end_date.month() {
        1 => "января",
        2 => "февраля",
        3 => "марта",
        4 => "апреля",
        5 => "мая",
        6 => "июня",
        7 => "июля",
        8 => "августа",
        9 => "сентября",
        10 => "октября",
        11 => "ноября",
        12 => "декабря",
        _ => "",
    };
    let end_year = end_date.year();
    
    if start_year == end_year && start_month == end_month {
        // Same month and year
        format!("с {} по {} {} {}", start_day, end_day, end_month, end_year)
    } else if start_year == end_year {
        // Same year, different months
        format!("с {} {} по {} {} {}", start_day, start_month, end_day, end_month, end_year)
    } else {
        // Different years
        format!("с {} {} {} по {} {} {}", 
                start_day, start_month, start_year, 
                end_day, end_month, end_year)
    }
}

async fn search_flights(
    client: &Client,
    origin: &str,
    destination: &str,
    departure_date: &str,
    api_key: &str,
) -> Result<FlightData, Box<dyn Error>> {
    // Updated to the latest API endpoint
    let url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates";
    
    let params = [
        ("origin", origin),
        ("destination", destination),
        ("departure_at", departure_date),
        ("return_at", ""),
        ("currency", "rub"),  // Using RUB as currency
        ("limit", "30"),      // Number of results
        ("page", "1"),
        ("one_way", "true"),  // No return flights
        ("direct", "true"),   // Only direct flights
        ("token", api_key),
    ];

    // Create request URL for logging without consuming the builder
    let request_url = {
        let temp_request = client.get(url).query(&params);
        temp_request.build()?.url().to_string()
    };
    println!("Searching flights from {} to {} on {}", origin, destination, departure_date);
    println!("Request URL: {}", request_url);

    // Create a fresh request
    let response = client
        .get(url)
        .query(&params)
        .send()
        .await?;
    
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        return Err(format!("API request failed with status {}: {}", status, text).into());
    }
    
    // Get the response body as text
    let response_text = response.text().await?;
    println!("Raw API Response: {}", response_text);
    
    // Try to directly parse the JSON response
    let flight_data: FlightData = match serde_json::from_str(&response_text) {
        Ok(data) => data,
        Err(e) => {
            println!("Error parsing JSON: {}", e);
            
            // Fallback to manual parsing
            let json_value: serde_json::Value = serde_json::from_str(&response_text)?;
            
            let success = json_value.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
            let currency = json_value.get("currency").and_then(|v| v.as_str()).map(|s| s.to_string());
            let error = json_value.get("error").and_then(|v| v.as_str()).map(|s| s.to_string());
            
            let mut flight_data = FlightData {
                success,
                data: None,
                currency,
                error,
            };
            
            if success && json_value.get("data").is_some() {
                let data = json_value.get("data").unwrap();
                if let Some(items) = data.as_array() {
                    let mut flights = Vec::new();
                    
                    for item in items {
                        let flight_result = FlightResult {
                            origin: item.get("origin").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            destination: item.get("destination").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            origin_airport: item.get("origin_airport").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            destination_airport: item.get("destination_airport").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            price: item.get("price").and_then(|v| v.as_i64()).unwrap_or(0),
                            airline: item.get("airline").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            flight_number: item.get("flight_number").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            departure_at: item.get("departure_at").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            return_at: item.get("return_at").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            transfers: item.get("transfers").and_then(|v| v.as_i64()).unwrap_or(0),
                            duration: item.get("duration").and_then(|v| v.as_i64()),
                            duration_to: item.get("duration_to").and_then(|v| v.as_i64()),
                            duration_back: item.get("duration_back").and_then(|v| v.as_i64()),
                            return_transfers: item.get("return_transfers").and_then(|v| v.as_i64()),
                            link: item.get("link").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            seats: item.get("seats").and_then(|v| v.as_i64()),
                        };
                        
                        flights.push(flight_result);
                    }
                    
                    flight_data.data = Some(flights);
                }
            }
            
            flight_data
        }
    };
    
    Ok(flight_data)
}

fn date_range(start_date: NaiveDate, end_date: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current_date = start_date;
    
    while current_date <= end_date {
        dates.push(current_date);
        current_date = current_date.succ_opt().unwrap();
    }
    
    dates
}

// Function to query AirLabs API for flight information
async fn get_airlabs_flight_info(
    client: &Client,
    airline_code: &str,
    flight_number: &str,
    api_key: &str,
) -> Result<Option<AirLabsFlight>, Box<dyn Error>> {
    // Build the AirLabs API URL
    let api_url = "https://airlabs.co/api/v9/flight";
    
    let params = [
        ("api_key", api_key),
        ("flight_iata", &format!("{}{}", airline_code, flight_number)),
    ];

    println!("Querying AirLabs API for flight: {}{}", airline_code, flight_number);
    
    // Make the request
    let response = client
        .get(api_url)
        .query(&params)
        .send()
        .await?;
    
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        eprintln!("AirLabs API request failed with status {}: {}", status, text);
        return Err(format!("AirLabs API request failed: {}", text).into());
    }
    
    // Parse the response
    let response_text = response.text().await?;
    println!("AirLabs API response: {}", response_text);
    
    let airlabs_response: serde_json::Value = serde_json::from_str(&response_text)?;
    
    // Check if there's an error
    if let Some(error) = airlabs_response.get("error") {
        if let Some(message) = error.get("message").and_then(|m| m.as_str()) {
            eprintln!("AirLabs API error: {}", message);
            return Err(format!("AirLabs API error: {}", message).into());
        }
    }
    
    // Check if we have response data
    if let Some(response_data) = airlabs_response.get("response") {
        if let Some(flights) = response_data.as_array() {
            if !flights.is_empty() {
                // Try to parse the first flight
                let flight: AirLabsFlight = serde_json::from_value(flights[0].clone())?;
                return Ok(Some(flight));
            }
        }
    }
    
    Ok(None)
}

// Function to enrich flight data with AirLabs information
async fn enrich_with_airlabs_data(
    client: &Client,
    flight: &FlightResult,
    airlabs_api_key: &str,
) -> Result<Option<AirLabsFlight>, Box<dyn Error>> {
    // Extract airline code and flight number
    let airline_code = &flight.airline;
    let flight_number = &flight.flight_number;
    
    // Query AirLabs API
    match get_airlabs_flight_info(client, airline_code, flight_number, airlabs_api_key).await {
        Ok(airlabs_flight) => Ok(airlabs_flight),
        Err(e) => {
            eprintln!("Error getting AirLabs data: {}", e);
            Ok(None)
        }
    }
}

// TODO: Create schedule checker for date from 15 sept 2025 to 30 sept 2025
// for available dates in the aero flights aviasales.ru each 6 hours
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load environment variables from .env file
    dotenv().ok();
    
    // Get API keys from environment variables
    let aviasales_api_key = env::var("TRAVELPAYOUTS_API_KEY")
        .expect("TRAVELPAYOUTS_API_KEY not found in environment variables");
    
    // Get Telegram bot token and chat ID from environment variables
    let telegram_bot_token = env::var("TELEGRAM_BOT_TOKEN")
        .unwrap_or_else(|_| {
            println!("TELEGRAM_BOT_TOKEN not found in environment variables. Notifications will not be sent.");
            String::new()
        });
    
    let telegram_chat_id = env::var("TELEGRAM_CHAT_ID")
        .unwrap_or_else(|_| {
            println!("TELEGRAM_CHAT_ID not found in environment variables. Notifications will not be sent.");
            String::new()
        });

    // Get general topic ID for flight notifications
    let telegram_devlogs_topic_id = env::var("TELEGRAM_DEVLOGS_TOPIC_ID")
        .unwrap_or_else(|_| {
            println!("TELEGRAM_DEVLOGS_TOPIC_ID not found in environment variables. Dev logs notifications will not be sent.");
            String::new()
        });
    let telegram_found_topic_id = env::var("TELEGRAM_FOUND_TOPIC_ID")
        .unwrap_or_else(|_| {
            println!("TELEGRAM_FOUND_TOPIC_ID not found in environment variables. Flight found notifications will not be sent.");
            String::new()
        });
    
    // Get AirLabs API key
    let airlabs_api_key = env::var("AIRLABS_API_KEY")
        .unwrap_or_else(|_| {
            println!("AIRLABS_API_KEY not found in environment variables. AirLabs enrichment will not be available.");
            String::new()
        });
    
    let enable_telegram = !telegram_bot_token.is_empty() && !telegram_chat_id.is_empty();
    let enable_secondary_notifications = !telegram_bot_token.is_empty() && !telegram_chat_id.is_empty();
    let enable_airlabs = !airlabs_api_key.is_empty();
    
    // Create HTTP client
    let client = Client::new();
    
    // Define search parameters
    let origin = env::var("ORIGIN")
    .unwrap_or_else(|_| {
        println!("ORIGIN not found in environment variables.");
        String::new()
    });; // Origin (all airports)
    let destination = env::var("DESTINATION")
    .unwrap_or_else(|_| {
        println!("DESTINATION not found in environment variables.");
        String::new()
    }); // Destination
    
    let start_date_env = env::var("START_DATE")
    .unwrap_or_else(|_| {
        println!("START_DATE not found in environment variables.");
        String::new()
    });
    let end_date_env = env::var("END_DATE")
    .unwrap_or_else(|_| {
        println!("END_DATE not found in environment variables.");
        String::new()
    });
    // Define date range
    let start_date = NaiveDate::parse_from_str(&start_date_env, "%Y-%m-%d")?;
    let end_date = NaiveDate::parse_from_str(&end_date_env, "%Y-%m-%d")?;
    
    // Create date range string for display
    let date_range_str = format_date_range_ru(&start_date, &end_date);
    
    // Send startup notification
    if enable_telegram {
        let origin_name = get_city_name(&origin);
        let destination_name = get_city_name(&destination);
        let startup_message = format!(
            "🛫 Программа поиска авиабилетов запущена!\n\n\
             Будет проверять прямые рейсы из {} в {} {}.\nПоиск будет происходить каждые 6 часов.",
            origin_name, destination_name, date_range_str
        );
        
        // Send startup message to all notification channels
        send_telegram_notification(
            &client, 
            &telegram_bot_token, 
            &telegram_chat_id, 
            &startup_message, 
            &telegram_devlogs_topic_id, 
            None
        ).await?;

    }
    
    let dates = date_range(start_date, end_date);
    
    // Check flights every 6 hours
    let hours_interval = 6;
    let check_interval = Duration::from_secs(hours_interval * 60 * 60);
    
    loop {
        let search_start_time = Utc::now();
        let formatted_start_time = format_utc_datetime_ru(search_start_time);
        println!("Starting flight search at {}", formatted_start_time);
        
        if enable_telegram {
            let cycle_start_message = format!("🔍 Начинаю цикл поиска рейсов {}", formatted_start_time);
            // Only send to devlogs topic
            send_telegram_notification(
                &client, 
                &telegram_bot_token, 
                &telegram_chat_id, 
                &cycle_start_message, 
                &telegram_devlogs_topic_id, 
                None
            ).await?;
        }
        
        for date in &dates {
            let departure_date = date.format("%Y-%m-%d").to_string();
            
            // Display the date in Russian format for logs
            let formatted_date = format!("{} {} {}", 
                date.day(), 
                match date.month() {
                    1 => "января",
                    2 => "февраля",
                    3 => "марта",
                    4 => "апреля",
                    5 => "мая",
                    6 => "июня",
                    7 => "июля",
                    8 => "августа",
                    9 => "сентября",
                    10 => "октября",
                    11 => "ноября",
                    12 => "декабря",
                    _ => "",
                },
                date.year()
            );
            
            match search_flights(&client, &origin, &destination, &departure_date, &aviasales_api_key).await {
                Ok(flight_data) => {
                    if flight_data.success {
                        if let Some(flights) = flight_data.data.as_ref() {
                            let flight_count = flights.len();
                            println!("Found {} flights for {}", flight_count, formatted_date);
                            
                            let origin_name = get_city_name(&origin);
                            let destination_name = get_city_name(&destination);
                            
                            if flight_count > 0 {
                                // Prepare flight details for Telegram
                                let mut telegram_message = format!("✅ Найдено <b>{} рейсов</b> на <b>{}</b> из {} в {}:\n\n", 
                                    flight_count, formatted_date, origin_name, destination_name);
                                
                                // Send to both primary and secondary chats if enabled
                                for (i, flight) in flights.iter().enumerate() {
                                    if i >= 5 {
                                        // Limit to 5 flights in a single message
                                        telegram_message.push_str(&format!("... и еще {} рейсов\n", flight_count - 5));
                                        break;
                                    }
                                    
                                    let origin_city = get_city_name(&flight.origin);
                                    let destination_city = get_city_name(&flight.destination);
                                    let airline_name = get_airline_name(&flight.airline);
                                    let formatted_departure = format_datetime_ru(&flight.departure_at);
                                    
                                    telegram_message.push_str(&format!(
                                        "🛫 <b>Рейс {}</b>: {} ({}) → {} ({})\n",
                                        flight.flight_number,
                                        origin_city,
                                        flight.origin_airport,
                                        destination_city,
                                        flight.destination_airport
                                    ));
                                    
                                    telegram_message.push_str(&format!(
                                        "⏰ <b>Вылет</b>: {}\n",
                                        formatted_departure
                                    ));
                                    
                                    telegram_message.push_str(&format!(
                                        "💰 <b>Примерная цена</b>: {} RUB\n",
                                        flight.price
                                    ));
                                    
                                    telegram_message.push_str(&format!(
                                        "✈️ <b>Авиакомпания</b>: {}, <b>Пересадок</b>: {}\n",
                                        airline_name,
                                        flight.transfers
                                    ));
                                    
                                    if let Some(seats) = flight.seats {
                                        telegram_message.push_str(&format!(
                                            "👥 <b>Доступно мест</b>: {}\n",
                                            seats
                                        ));
                                    }
                                    
                                    if let Some(duration) = flight.duration {
                                        telegram_message.push_str(&format!(
                                            "⏱ <b>Длительность</b>: {}\n",
                                            format_duration(duration)
                                        ));
                                    }
                                    
                                    // Create inline keyboard for this flight
                                    let keyboard = json!({
                                        "inline_keyboard": [
                                            [
                                                {
                                                    "text": "Забронировать билет",
                                                    "url": format!("https://aviasales.ru{}", flight.link)
                                                }
                                            ]
                                        ]
                                    });
                                    
                                    // Send flight notifications to all relevant topics
                                    if enable_telegram {
                                        // Important flight finds go to both topics
                                        let topic_ids = vec![telegram_devlogs_topic_id.clone(), telegram_found_topic_id.clone()];
                                        send_telegram_multi_topic_notification(
                                        &client, 
                                        &telegram_bot_token, 
                                        &telegram_chat_id, 
                                        &telegram_message,
                                            &topic_ids,
                                            Some(keyboard.clone())
                                    ).await?;
                                    }
                                    
                                    // Clear message for next flight
                                    telegram_message = String::new();
                                }
                                
                                // Now process AirLabs data for each flight if enabled
                                if enable_airlabs {
                                    for flight in flights {
                                        match enrich_with_airlabs_data(&client, flight, &airlabs_api_key).await {
                                            Ok(Some(airlabs_flight)) => {
                                                // ... existing AirLabs processing code ...
                                                
                                                // Send AirLabs data to both chat IDs if seat info is available
                                                let mut has_seat_info = false;
                                                let mut airlabs_message = String::new();
                                                
                                                airlabs_message.push_str(&format!(
                                                    "📊 <b>Дополнительная информация для рейса {}{}</b>:\n",
                                                    flight.airline, flight.flight_number
                                                ));
                                                
                                                if let Some(status) = &airlabs_flight.status {
                                                    airlabs_message.push_str(&format!("🚦 <b>Статус рейса</b>: {}\n", status));
                                                }
                                                
                                                if let Some(aircraft) = &airlabs_flight.aircraft_icao {
                                                    airlabs_message.push_str(&format!("✈️ <b>Тип самолета</b>: {}\n", aircraft));
                                                }
                                                
                                                if let Some(economy) = airlabs_flight.seats_economy {
                                                    airlabs_message.push_str(&format!("💺 <b>Мест в эконом-классе</b>: {}\n", economy));
                                                    has_seat_info = true;
                                                }
                                                
                                                if let Some(business) = airlabs_flight.seats_business {
                                                    airlabs_message.push_str(&format!("💺 <b>Мест в бизнес-классе</b>: {}\n", business));
                                                    has_seat_info = true;
                                                }
                                                
                                                if let Some(first) = airlabs_flight.seats_first {
                                                    airlabs_message.push_str(&format!("💺 <b>Мест в первом классе</b>: {}\n", first));
                                                    has_seat_info = true;
                                                }
                                                
                                                if !airlabs_message.is_empty() {
                                                    // Send to primary chat ID
                                                    if enable_telegram {
                                                        send_telegram_notification(
                                                            &client,
                                                            &telegram_bot_token,
                                                            &telegram_chat_id,
                                                            &airlabs_message,
                                                            &telegram_found_topic_id,
                                                            None
                                                        ).await?;
                                                    }
                                                    
                                                    // Send to secondary chat ID if has seat info
                                                    if enable_secondary_notifications && has_seat_info {
                                                        let secondary_airlabs_message = format!(
                                                            "🚨 <b>ИНФОРМАЦИЯ О НАЛИЧИИ МЕСТ:</b> 🚨\n\n{}",
                                                            airlabs_message
                                                        );
                                                        
                                                        send_telegram_notification(
                                                            &client,
                                                            &telegram_bot_token,
                                                            &telegram_chat_id,
                                                            &secondary_airlabs_message,
                                                            &telegram_found_topic_id,
                                                            None
                                                        ).await?;
                                                    }
                                                }
                                            },
                                            Ok(None) => {
                                                println!("No AirLabs data found for flight {}{}", 
                                                    flight.airline, flight.flight_number);
                                            },
                                            Err(e) => {
                                                eprintln!("Error fetching AirLabs data: {}", e);
                                            }
                                        }
                                    }
                                }
                            } else {
                                println!("No flights found for {}", formatted_date);
                                
                                if enable_telegram {
                                    let origin_name = get_city_name(&origin);
                                    let destination_name = get_city_name(&destination);
                                    let no_flights_message = format!("ℹ️ Рейсы не найдены на {} из {} в {}", 
                                        formatted_date, origin_name, destination_name);
                                    // Only send to devlogs topic
                                    send_telegram_notification(
                                        &client, 
                                        &telegram_bot_token, 
                                        &telegram_chat_id, 
                                        &no_flights_message, 
                                        &telegram_devlogs_topic_id,
                                        None
                                    ).await?;
                                }
                            }
                        } else {
                            println!("No flights found for {}", formatted_date);
                            
                            if enable_telegram {
                                let origin_name = get_city_name(&origin);
                                let destination_name = get_city_name(&destination);
                                let no_flights_message = format!("ℹ️ Рейсы не найдены на {} из {} в {}", 
                                    formatted_date, origin_name, destination_name);
                                // Only send to devlogs topic
                                send_telegram_notification(
                                    &client, 
                                    &telegram_bot_token, 
                                    &telegram_chat_id, 
                                    &no_flights_message, 
                                    &telegram_devlogs_topic_id,
                                    None
                                ).await?;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error searching flights for {}: {}", formatted_date, e);
                    
                    if enable_telegram {
                        let error_message = format!("❌ Ошибка при поиске рейсов на {}: {}", 
                            formatted_date, e);
                        send_telegram_notification(&client, &telegram_bot_token, &telegram_chat_id, &error_message, &telegram_devlogs_topic_id, None).await?;
                    }
                }
            }
            
            // Add a small delay between API calls to avoid rate limiting
            time::sleep(Duration::from_secs(1)).await;
        }
        
        let search_end_time = Utc::now();
        let formatted_end_time = format_utc_datetime_ru(search_end_time);
        println!("Completed flight search cycle at {}. Waiting {} hours before next check.", formatted_end_time, hours_interval);
        
        if enable_telegram {
            let cycle_end_message = format!(
                "✅ Цикл поиска прямых рейсов завершен {}.\n\
                 Следующая проверка будет через {} часов.",
                formatted_end_time, hours_interval
            );
            // Only send to devlogs topic
            send_telegram_notification(
                &client, 
                &telegram_bot_token, 
                &telegram_chat_id, 
                &cycle_end_message, 
                &telegram_devlogs_topic_id, 
                None
            ).await?;
        }
        
        time::sleep(check_interval).await;
    }
}
