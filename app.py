from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import sqlite3
import math
import random
import json
from datetime import datetime, timedelta
import os

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

DB_PATH = os.path.join(os.path.dirname(__file__), '../database/hotels.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Price Optimizer Engine ───────────────────────────────────────────────────

def calculate_dynamic_price(base_price, weather_score, demand_score, supply_factor, days_ahead):
    """
    Dynamic pricing model combining all factors:
    - weather_score: 0.0 (terrible) to 1.0 (perfect)
    - demand_score: 0.0 (low) to 1.0 (very high)
    - supply_factor: ratio of available rooms (0.0 = fully booked, 1.0 = empty)
    - days_ahead: how many days until check-in (urgency)
    """
    # Weather premium: good weather = higher price
    weather_multiplier = 0.85 + (weather_score * 0.35)

    # Demand multiplier (exponential at high demand)
    demand_multiplier = 0.75 + (demand_score ** 1.4) * 0.65

    # Supply scarcity: low supply = higher price
    supply_multiplier = 1.0 + max(0, (0.3 - supply_factor) * 1.8)

    # Time urgency: last-minute boosting or early-bird discount
    if days_ahead <= 2:
        time_multiplier = 1.25  # Last minute surge
    elif days_ahead <= 7:
        time_multiplier = 1.10
    elif days_ahead >= 60:
        time_multiplier = 0.88  # Early bird discount
    else:
        time_multiplier = 1.0

    final_price = base_price * weather_multiplier * demand_multiplier * supply_multiplier * time_multiplier
    return round(final_price, 2)


def get_weather_data(lat, lon, date_offset=0):
    """Simulate weather data based on location and date"""
    # Seasonal variation using lat and month
    month = (datetime.now() + timedelta(days=date_offset)).month
    season_factor = math.sin((month - 3) * math.pi / 6)  # peaks in summer

    # Latitude effect (tropical vs polar)
    lat_factor = 1.0 - abs(lat) / 90.0

    base_temp = 20 + (lat_factor * 15) + (season_factor * 10)
    temp = base_temp + random.uniform(-3, 3)
    
    # Precipitation inverse of score
    rain_prob = max(0, 0.4 - lat_factor * 0.3 + random.uniform(-0.1, 0.1))
    
    weather_score = (1 - rain_prob) * 0.6 + (min(temp, 30) / 30) * 0.4
    weather_score = max(0.1, min(1.0, weather_score))

    conditions = ["Sunny", "Partly Cloudy", "Overcast", "Light Rain", "Clear Skies"]
    condition = conditions[int((1 - weather_score) * 4)]

    return {
        "temperature": round(temp, 1),
        "precipitation": round(rain_prob * 100, 1),
        "condition": condition,
        "score": round(weather_score, 3),
        "humidity": round(40 + (rain_prob * 40) + random.uniform(-5, 5), 1),
        "wind_speed": round(random.uniform(5, 30), 1)
    }


def get_demand_score(hotel_id, date_offset=0):
    """Simulate demand based on day of week, season, events"""
    check_date = datetime.now() + timedelta(days=date_offset)
    dow = check_date.weekday()  # 0=Mon, 6=Sun
    month = check_date.month

    # Weekend premium
    weekend_factor = 0.3 if dow >= 4 else 0.0

    # High season (summer + winter holidays)
    if month in [6, 7, 8]:
        season_factor = 0.4
    elif month in [12, 1]:
        season_factor = 0.35
    else:
        season_factor = 0.1

    # Random event factor
    event_factor = random.choice([0, 0, 0, 0.3, 0.5])  # 40% chance of event

    demand = 0.3 + weekend_factor + season_factor + event_factor + random.uniform(-0.05, 0.05)
    return round(min(1.0, max(0.1, demand)), 3)


# ─── API Routes ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('../frontend', 'index.html')

@app.route('/api/hotels/search', methods=['GET'])
def search_hotels():
    lat = float(request.args.get('lat', 20.2961))
    lon = float(request.args.get('lon', 85.8245))
    radius = float(request.args.get('radius', 50))  # km
    check_in_days = int(request.args.get('days_ahead', 1))
    guests = int(request.args.get('guests', 2))

    conn = get_db()
    hotels_raw = conn.execute("""
        SELECT * FROM hotels 
        WHERE (
            (6371 * acos(cos(radians(?)) * cos(radians(lat)) *
            cos(radians(lon) - radians(?)) + sin(radians(?)) * sin(radians(lat))))
        ) <= ?
        ORDER BY star_rating DESC
        LIMIT 20
    """, (lat, lon, lat, radius)).fetchall()
    conn.close()

    hotels = []
    for h in hotels_raw:
        h = dict(h)
        weather = get_weather_data(h['lat'], h['lon'], check_in_days)
        demand = get_demand_score(h['id'], check_in_days)
        supply = random.uniform(0.1, 0.9)

        optimized_price = calculate_dynamic_price(
            h['base_price'], weather['score'], demand, supply, check_in_days
        )

        price_change = ((optimized_price - h['base_price']) / h['base_price']) * 100

        h['optimized_price'] = optimized_price
        h['price_change_pct'] = round(price_change, 1)
        h['weather'] = weather
        h['demand_score'] = demand
        h['supply_factor'] = round(supply, 3)
        h['available_rooms'] = int(h['total_rooms'] * supply)
        h['price_factors'] = {
            'weather': round(weather['score'], 2),
            'demand': round(demand, 2),
            'supply': round(1 - supply, 2),
            'time_urgency': "High" if check_in_days <= 2 else "Medium" if check_in_days <= 7 else "Low"
        }
        hotels.append(h)

    return jsonify({"hotels": hotels, "total": len(hotels)})


@app.route('/api/hotels/<int:hotel_id>/price-forecast', methods=['GET'])
def price_forecast(hotel_id):
    conn = get_db()
    hotel = conn.execute("SELECT * FROM hotels WHERE id = ?", (hotel_id,)).fetchone()
    conn.close()

    if not hotel:
        return jsonify({"error": "Hotel not found"}), 404

    hotel = dict(hotel)
    forecast = []

    for day_offset in range(0, 30):
        weather = get_weather_data(hotel['lat'], hotel['lon'], day_offset)
        demand = get_demand_score(hotel_id, day_offset)
        supply = random.uniform(0.15, 0.85)

        price = calculate_dynamic_price(
            hotel['base_price'], weather['score'], demand, supply, day_offset
        )

        date = (datetime.now() + timedelta(days=day_offset)).strftime('%Y-%m-%d')
        forecast.append({
            "date": date,
            "price": price,
            "weather_score": weather['score'],
            "demand": demand,
            "supply": round(supply, 2),
            "condition": weather['condition'],
            "temp": weather['temperature']
        })

    return jsonify({"hotel_id": hotel_id, "hotel_name": hotel['name'], "forecast": forecast})


@app.route('/api/hotels/<int:hotel_id>', methods=['GET'])
def get_hotel(hotel_id):
    conn = get_db()
    hotel = conn.execute("SELECT * FROM hotels WHERE id = ?", (hotel_id,)).fetchone()
    reviews = conn.execute("""
        SELECT * FROM reviews WHERE hotel_id = ? ORDER BY created_at DESC LIMIT 5
    """, (hotel_id,)).fetchall()
    conn.close()

    if not hotel:
        return jsonify({"error": "Not found"}), 404

    hotel = dict(hotel)
    hotel['reviews'] = [dict(r) for r in reviews]
    hotel['amenities'] = json.loads(hotel.get('amenities', '[]'))

    weather = get_weather_data(hotel['lat'], hotel['lon'])
    demand = get_demand_score(hotel_id)
    supply = random.uniform(0.2, 0.8)

    hotel['current_weather'] = weather
    hotel['current_demand'] = demand
    hotel['current_price'] = calculate_dynamic_price(
        hotel['base_price'], weather['score'], demand, supply, 1
    )

    return jsonify(hotel)


@app.route('/api/analytics/market', methods=['GET'])
def market_analytics():
    conn = get_db()
    hotels = conn.execute("SELECT * FROM hotels").fetchall()
    conn.close()

    total_revenue = 0
    avg_occupancy = 0
    price_distribution = {"budget": 0, "mid": 0, "luxury": 0}

    hotel_data = []
    for h in hotels:
        h = dict(h)
        supply = random.uniform(0.2, 0.8)
        occupancy = (1 - supply) * 100
        avg_occupancy += occupancy
        revenue = h['base_price'] * h['total_rooms'] * (1 - supply)
        total_revenue += revenue

        if h['base_price'] < 5000:
            price_distribution['budget'] += 1
        elif h['base_price'] < 12000:
            price_distribution['mid'] += 1
        else:
            price_distribution['luxury'] += 1

        hotel_data.append({
            "name": h['name'],
            "occupancy": round(occupancy, 1),
            "revenue": round(revenue, 0),
            "stars": h['star_rating']
        })

    return jsonify({
        "total_hotels": len(hotels),
        "avg_occupancy": round(avg_occupancy / max(len(hotels), 1), 1),
        "total_revenue_estimate": round(total_revenue, 0),
        "price_distribution": price_distribution,
        "top_hotels": sorted(hotel_data, key=lambda x: x['revenue'], reverse=True)[:5]
    })


@app.route('/api/spark/optimize', methods=['POST'])
def spark_optimize():
    """Endpoint that simulates Spark distributed processing results"""
    data = request.json
    city = data.get('city', 'Bhubaneswar')

    # Simulate Spark job results
    spark_result = {
        "job_id": f"spark_job_{random.randint(10000, 99999)}",
        "city": city,
        "processing_time_ms": random.randint(800, 2400),
        "records_processed": random.randint(50000, 200000),
        "partitions_used": 8,
        "optimization_insights": [
            {
                "insight": "Peak demand window detected: Friday 6PM – Sunday 11PM",
                "revenue_impact": "+18.4%",
                "confidence": 0.92
            },
            {
                "insight": "Competitor pricing 12% lower during monsoon season",
                "revenue_impact": "-7.2%",
                "confidence": 0.87
            },
            {
                "insight": "Weather-correlated demand spike: 3-day forecast sunny",
                "revenue_impact": "+9.1%",
                "confidence": 0.78
            },
            {
                "insight": "Early-bird bookings 60+ days out show 22% higher LTV",
                "revenue_impact": "+5.6%",
                "confidence": 0.95
            }
        ],
        "recommended_base_price_adjustment": f"+{random.randint(3,15)}%",
        "optimal_overbooking_rate": f"{random.randint(5,12)}%"
    }

    return jsonify(spark_result)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
