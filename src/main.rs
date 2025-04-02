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
        // Convert to local time (UTC+5)
        let local_time = dt.with_timezone(&FixedOffset::east_opt(5 * 3600).unwrap());
        
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
    // Convert to UTC+5
    let local_time = dt.with_timezone(&FixedOffset::east_opt(5 * 3600).unwrap());
    
    // Format in Russian
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
    let second = local_time.second();
    
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

// Add these new structs to track search statistics
#[derive(Debug, Default)]
struct SearchStatistics {
    total_dates_checked: usize,
    dates_with_flights: usize,
    dates_without_flights: usize,
    total_flights_found: usize,
    errors_encountered: usize,
    flight_dates: Vec<(String, String)>, // (date, message_id)
}

impl SearchStatistics {
    fn new() -> Self {
        Self::default()
    }

    fn format_summary(&self) -> String {
        let mut summary = format!(
            "📊 <b>Статистика поиска:</b>\n\
             ✓ Проверено дат: {}\n\
             ✈️ Даты с рейсами: {}\n\
             ❌ Даты без рейсов: {}\n\
             🎫 Всего найдено рейсов: {}\n\
             ⚠️ Ошибок: {}\n",
            self.total_dates_checked,
            self.dates_with_flights,
            self.dates_without_flights,
            self.total_flights_found,
            self.errors_encountered
        );
        
        if !self.flight_dates.is_empty() {
            summary.push_str("\n<b>Даты с найденными рейсами:</b>\n");
            for (date, message_id) in &self.flight_dates {
                summary.push_str(&format!("• <a href=\"https://t.me/c/{}/{}\">{}</a>\n", 
                    message_id.split('/').nth(0).unwrap_or(""),
                    message_id.split('/').nth(1).unwrap_or(""),
                    date
                ));
            }
        }
        
        summary
    }
}

// Add this function to update a Telegram message
async fn update_telegram_message(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    message_id: &str,
    message: &str,
    topic_id: &str,
) -> Result<(), Box<dyn Error>> {
    let api_url = format!("https://api.telegram.org/bot{}/editMessageText", bot_token);
    
    let mut json_body = json!({
        "chat_id": chat_id,
        "message_id": message_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": true
    });

    // Add message_thread_id only if topic_id is not empty and not "1"
    if !topic_id.is_empty() && topic_id != "1" {
        json_body["message_thread_id"] = json!(topic_id);
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
            // Add a small delay to avoid Telegram rate limits
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
            } else {
                // Other error, not rate limiting
                eprintln!("Telegram API request failed with status {}: {}", status, text);
                return Err(format!("Telegram API request failed: {}", text).into());
            }
        }
    }
}

// Function to send a message and return the message ID
async fn send_telegram_notification_with_id(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    message: &str,
    topic_id: &str,
    inline_keyboard: Option<serde_json::Value>,
) -> Result<String, Box<dyn Error>> {
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
            // Parse the response to get the message ID
            let response_text = response.text().await?;
            let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
            
            let message_id = response_json
                .get("result")
                .and_then(|result| result.get("message_id"))
                .and_then(|id| id.as_i64())
                .ok_or("Failed to get message ID from Telegram response")?;
            
            // Add a small delay to avoid Telegram rate limits
            time::sleep(Duration::from_millis(1000)).await;
            return Ok(message_id.to_string());
        } else {
            // ... existing error handling ...
            // Same as in send_telegram_notification function
            let status = response.status();
            let text = response.text().await?;
                
            if status.as_u16() == 429 {
                retry_count += 1;
                
                if retry_count > max_retries {
                    return Err(format!("Exceeded maximum retries for Telegram API. Last error: {}", text).into());
                }
                
                let retry_after = if let Ok(error_json) = serde_json::from_str::<serde_json::Value>(&text) {
                    error_json.get("parameters")
                        .and_then(|p| p.get("retry_after"))
                        .and_then(|r| r.as_f64())
                        .unwrap_or_else(|| {
                            let backoff = initial_delay * 2_u64.pow(retry_count as u32);
                            backoff as f64
                        })
                } else {
                    let backoff = initial_delay * 2_u64.pow(retry_count as u32);
                    backoff as f64
                };
                
                let wait_time = Duration::from_secs_f64(retry_after);
                eprintln!("Telegram API rate limited (429). Waiting for {} seconds before retry {}/{}...", 
                    wait_time.as_secs(), retry_count, max_retries);
                
                time::sleep(wait_time).await;
            } else {
                eprintln!("Telegram API request failed with status {}: {}", status, text);
                return Err(format!("Telegram API request failed: {}", text).into());
            }
        }
    }
}

// Add this new function to check for previous messages
async fn get_previous_messages(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    topic_id: &str,
    limit: i32,
) -> Result<Vec<String>, Box<dyn Error>> {
    let api_url = format!("https://api.telegram.org/bot{}/getChatHistory", bot_token);
    
    let mut json_body = json!({
        "chat_id": chat_id,
        "limit": limit
    });

    if !topic_id.is_empty() && topic_id != "1" {
        json_body["message_thread_id"] = json!(topic_id);
    }
    
    let response = client
        .post(&api_url)
        .json(&json_body)
        .send()
        .await?;
    
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        return Err(format!("Failed to get chat history: {} - {}", status, text).into());
    }
    
    let response_text = response.text().await?;
    let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
    
    let mut message_ids = Vec::new();
    if let Some(messages) = response_json.get("result").and_then(|r| r.as_array()) {
        for message in messages {
            if let Some(message_id) = message.get("message_id").and_then(|id| id.as_i64()) {
                message_ids.push(message_id.to_string());
            }
        }
    }
    
    Ok(message_ids)
}

// Add this function to check if a message was sent in the last 48 hours
async fn was_message_sent_recently(
    client: &Client,
    bot_token: &str,
    chat_id: &str,
    topic_id: &str,
    message_text: &str,
) -> Result<bool, Box<dyn Error>> {
    // Get messages from the last 48 hours
    let previous_messages = get_previous_messages(client, bot_token, chat_id, topic_id, 100).await?;
    
    // Check if a similar message exists
    for message_id in previous_messages {
        let api_url = format!("https://api.telegram.org/bot{}/getMessage", bot_token);
        let json_body = json!({
            "chat_id": chat_id,
            "message_id": message_id
        });
        
        let response = client
            .post(&api_url)
            .json(&json_body)
            .send()
            .await?;
        
        if response.status().is_success() {
            let response_text = response.text().await?;
            let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
            
            if let Some(message) = response_json.get("result").and_then(|r| r.get("text")) {
                if let Some(text) = message.as_str() {
                    // Compare the message text (ignoring timestamps and dynamic content)
                    if text.contains(message_text) {
                        return Ok(true);
                    }
                }
            }
        }
    }
    
    Ok(false)
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
    }); // Origin (all airports)
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
    
    // Initialize statistics tracking
    let mut stats = SearchStatistics::new();
    let mut status_message_id: Option<String> = None;

    let dates = date_range(start_date, end_date);
    
    // Check flights every 6 hours
    let hours_interval = 6;
    let check_interval = Duration::from_secs(hours_interval);
    
    // Send startup notification
    if enable_telegram {
        let origin_name = get_city_name(&origin);
        let destination_name = get_city_name(&destination);
        let startup_message = format!(
            "🛫 <b>Программа поиска авиабилетов запущена!</b>\n\n\
             Будет проверять прямые рейсы из <b>{}</b> в <b>{}</b> {}.\n\
             Поиск будет происходить каждые {} часов.\n\n\
             <i>Этот статус будет обновляться с результатами поиска.</i>",
            origin_name, destination_name, date_range_str, hours_interval
        );
        
        // Send startup message and store message ID
        match send_telegram_notification_with_id(
            &client, 
            &telegram_bot_token, 
            &telegram_chat_id, 
            &startup_message, 
            &telegram_devlogs_topic_id, 
            None
        ).await {
            Ok(message_id) => {
                status_message_id = Some(message_id.clone());
                println!("Status message created with ID: {}", message_id);
            },
            Err(e) => {
                eprintln!("Failed to send initial status message: {}", e);
            }
        }
    }
    
    loop {
        // Reset statistics for this search cycle
        stats = SearchStatistics::new();
        
        let search_start_time = Utc::now();
        let formatted_start_time = format_utc_datetime_ru(search_start_time);
        println!("Starting flight search at {}", formatted_start_time);
        
        if enable_telegram && status_message_id.is_some() {
            let cycle_start_message = format!(
                "🛫 <b>Программа поиска авиабилетов</b>\n\n\
                🔍 Начат цикл поиска рейсов: {}\n\
                🗓 Проверяемые даты: {}\n\n\
                <i>Статус будет обновляться...</i>",
                formatted_start_time, date_range_str
            );
            
            // Update status message
            if let Err(e) = update_telegram_message(
                &client, 
                &telegram_bot_token, 
                &telegram_chat_id, 
                &status_message_id.as_ref().unwrap(), 
                &cycle_start_message, 
                &telegram_devlogs_topic_id
            ).await {
                eprintln!("Failed to update status message: {}", e);
            }
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
            
            // Update statistics for checked date
            stats.total_dates_checked += 1;
            
            match search_flights(&client, &origin, &destination, &departure_date, &aviasales_api_key).await {
                Ok(flight_data) => {
                    if flight_data.success {
                        if let Some(flights) = flight_data.data.as_ref() {
                            let flight_count = flights.len();
                            println!("Found {} flights for {}", flight_count, formatted_date);
                            
                            let origin_name = get_city_name(&origin);
                            let destination_name = get_city_name(&destination);
                            
                            if flight_count > 0 {
                                // Update statistics
                                stats.dates_with_flights += 1;
                                stats.total_flights_found += flight_count;
                                
                                // Check if a similar message was sent recently
                                let message_text = format!("Найдено {} рейсов на {}", flight_count, formatted_date);
                                let was_recent = was_message_sent_recently(
                                    &client,
                                    &telegram_bot_token,
                                    &telegram_chat_id,
                                    &telegram_found_topic_id,
                                    &message_text
                                ).await?;
                                
                                if !was_recent {
                                    let message_id = send_telegram_notification_with_id(
                                        &client,
                                        &telegram_bot_token,
                                        &telegram_chat_id,
                                        &format!("✅ Найдено <b>{} рейсов</b> на <b>{}</b> из {} в {}:\n\n", 
                                            flight_count, formatted_date, origin_name, destination_name),
                                        &telegram_found_topic_id,
                                        None
                                    ).await?;
                                    
                                    // Update statistics with message ID
                                    stats.flight_dates.push((formatted_date.clone(), message_id));
                                    
                                    // Send flight details
                                    for (i, flight) in flights.iter().enumerate() {
                                        if i >= 5 {
                                            // Limit to 5 flights in a single message
                                            let message_text = format!("... и еще {} рейсов", flight_count - 5);
                                            let was_recent = was_message_sent_recently(
                                                &client,
                                                &telegram_bot_token,
                                                &telegram_chat_id,
                                                &telegram_found_topic_id,
                                                &message_text
                                            ).await?;
                                            
                                            if !was_recent {
                                                let message_id = send_telegram_notification_with_id(
                                                    &client,
                                                    &telegram_bot_token,
                                                    &telegram_chat_id,
                                                    &message_text,
                                                    &telegram_found_topic_id,
                                                    None
                                                ).await?;
                                            }
                                            break;
                                        }
                                        
                                        let origin_city = get_city_name(&flight.origin);
                                        let destination_city = get_city_name(&flight.destination);
                                        let airline_name = get_airline_name(&flight.airline);
                                        let formatted_departure = format_datetime_ru(&flight.departure_at);
                                        
                                        let message_text = format!(
                                            "🛫 <b>Рейс {}</b>: {} ({}) → {} ({})\n",
                                            flight.flight_number,
                                            origin_city,
                                            flight.origin_airport,
                                            destination_city,
                                            flight.destination_airport
                                        );
                                        
                                        let was_recent = was_message_sent_recently(
                                            &client,
                                            &telegram_bot_token,
                                            &telegram_chat_id,
                                            &telegram_found_topic_id,
                                            &message_text
                                        ).await?;
                                        
                                        if !was_recent {
                                            send_telegram_notification(
                                                &client,
                                                &telegram_bot_token,
                                                &telegram_chat_id,
                                                &message_text,
                                                &telegram_found_topic_id,
                                                None
                                            ).await?;
                                        }
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
                                    // Update statistics
                                    stats.dates_without_flights += 1;
                                    println!("No flights found for {}", formatted_date);
                                }
                            } else {
                                // Update statistics
                                stats.dates_without_flights += 1;
                                println!("No flights found for {}", formatted_date);
                            }
                        } else {
                            // Update statistics
                            stats.dates_without_flights += 1;
                            println!("No flights found for {}", formatted_date);
                        }
                    }
                }
                Err(e) => {
                    // Update statistics for error
                    stats.errors_encountered += 1;
                    eprintln!("Error searching flights for {}: {}", formatted_date, e);
                    
                    // Send a separate error message
                    if enable_telegram {
                        let error_message = format!(
                            "⚠️ <b>Ошибка при поиске рейсов</b>\n\n\
                            📅 Дата: {}\n\
                            ❌ Ошибка: {}\n\n\
                            <i>Поиск продолжается...</i>",
                            formatted_date,
                            e
                        );
                        
                        if let Err(send_err) = send_telegram_notification(
                            &client,
                            &telegram_bot_token,
                            &telegram_chat_id,
                            &error_message,
                            &telegram_devlogs_topic_id,
                            None
                        ).await {
                            eprintln!("Failed to send error message: {}", send_err);
                        }
                    }
                    
                    // Update status message without the error details
                    if enable_telegram && status_message_id.is_some() {
                        let progress_message = format!(
                            "🛫 <b>Программа поиска авиабилетов</b>\n\n\
                            🔍 Поиск начат: {}\n\
                            🗓 Проверяемые даты: {}\n\n\
                            {}\n\n\
                            <i>Поиск в процессе ({}/{} дат проверено)...</i>",
                            formatted_start_time,
                            date_range_str,
                            stats.format_summary(),
                            stats.total_dates_checked,
                            dates.len()
                        );
                        
                        if let Err(update_err) = update_telegram_message(
                            &client,
                            &telegram_bot_token,
                            &telegram_chat_id,
                            &status_message_id.as_ref().unwrap(),
                            &progress_message,
                            &telegram_devlogs_topic_id
                        ).await {
                            eprintln!("Failed to update status message: {}", update_err);
                        }
                    }
                }
            }
            
            // Add a small delay between API calls to avoid rate limiting
            time::sleep(Duration::from_secs(1)).await;
        }
        
        let search_end_time = Utc::now();
        let formatted_end_time = format_utc_datetime_ru(search_end_time);
        let duration = search_end_time.signed_duration_since(search_start_time);
        let duration_minutes = duration.num_minutes();
        let duration_seconds = duration.num_seconds();
        
        println!("Completed flight search cycle at {}. Waiting {} hours before next check.", formatted_end_time, hours_interval);
        
        // Final status update with complete statistics
        if enable_telegram && status_message_id.is_some() {
            let final_message = format!(
                "🛫 <b>Программа поиска авиабилетов</b>\n\n\
                ✅ <b>Цикл поиска завершен!</b>\n\
                🕒 Начало: {}\n\
                🕕 Окончание: {}\n\
                ⏱ Длительность: {} минут {} секунд\n\
                🗓 Проверено дат: {}\n\n\
                {}\n\n\
                🔄 Следующий цикл через <b>{} часов</b>",
                formatted_start_time,
                formatted_end_time,
                duration_minutes,
                duration_seconds,
                dates.len(),
                stats.format_summary(),
                hours_interval
            );
            
            if let Err(e) = update_telegram_message(
                &client,
                &telegram_bot_token,
                &telegram_chat_id,
                &status_message_id.as_ref().unwrap(),
                &final_message,
                &telegram_devlogs_topic_id
            ).await {
                eprintln!("Failed to update final status message: {}", e);
            }
        }
        
        time::sleep(check_interval).await;
    }
}
