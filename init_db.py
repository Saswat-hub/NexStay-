import sqlite3
import json
import os
import random
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), 'hotels.db')

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Drop existing tables
    c.execute("DROP TABLE IF EXISTS reviews")
    c.execute("DROP TABLE IF EXISTS bookings")
    c.execute("DROP TABLE IF EXISTS hotels")
    c.execute("DROP TABLE IF EXISTS price_history")

    # Hotels table
    c.execute("""
        CREATE TABLE hotels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            city TEXT NOT NULL,
            address TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            star_rating INTEGER NOT NULL,
            base_price REAL NOT NULL,
            total_rooms INTEGER NOT NULL,
            amenities TEXT DEFAULT '[]',
            description TEXT,
            image_url TEXT,
            rating REAL DEFAULT 4.0,
            review_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Reviews table
    c.execute("""
        CREATE TABLE reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_id INTEGER NOT NULL,
            reviewer_name TEXT,
            rating REAL NOT NULL,
            comment TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hotel_id) REFERENCES hotels(id)
        )
    """)

    # Bookings table
    c.execute("""
        CREATE TABLE bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_id INTEGER NOT NULL,
            guest_name TEXT,
            check_in DATE,
            check_out DATE,
            rooms INTEGER DEFAULT 1,
            price_paid REAL,
            status TEXT DEFAULT 'confirmed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hotel_id) REFERENCES hotels(id)
        )
    """)

    # Price history table
    c.execute("""
        CREATE TABLE price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_id INTEGER NOT NULL,
            recorded_date DATE,
            price REAL,
            occupancy_rate REAL,
            weather_score REAL,
            demand_score REAL,
            FOREIGN KEY (hotel_id) REFERENCES hotels(id)
        )
    """)

    # ─── Seed Hotels Data ─────────────────────────────────────────────────────

    hotels = [
        # Bhubaneswar / Odisha
        ("Mayfair Lagoon", "Bhubaneswar", "8B, Jaydev Vihar, Bhubaneswar, Odisha 751013",
         20.2961, 85.8245, 5, 8500, 80,
         '["Pool", "Spa", "Restaurant", "Bar", "Gym", "Conference Hall"]',
         "Luxury resort with lagoon pool and lush gardens in the heart of Bhubaneswar.",
         4.6, 342),

        ("Trident Bhubaneswar", "Bhubaneswar", "CB-1, Nayapalli, Bhubaneswar 751015",
         20.2883, 85.8197, 5, 9200, 60,
         '["Pool", "Spa", "Fine Dining", "Business Center", "Valet Parking"]',
         "Five-star retreat offering contemporary luxury and exceptional hospitality.",
         4.7, 210),

        ("The Crown", "Bhubaneswar", "Plot No.1, Saheed Nagar, Bhubaneswar 751007",
         20.2735, 85.8442, 4, 4800, 45,
         '["Restaurant", "Bar", "Gym", "Banquet Hall"]',
         "Modern comfort hotel in central Bhubaneswar with excellent dining facilities.",
         4.2, 178),

        ("Hotel Hindustan International", "Bhubaneswar", "Kalpana Square, Bhubaneswar 751014",
         20.2640, 85.8397, 4, 5200, 55,
         '["Restaurant", "Conference Room", "Bar", "Room Service"]',
         "Business-friendly hotel with comfortable rooms and great connectivity.",
         4.1, 289),

        ("Swosti Grand", "Bhubaneswar", "103, Janpath, Bhubaneswar 751001",
         20.2680, 85.8411, 4, 6100, 70,
         '["Restaurant", "Banquet", "Gym", "Business Center", "Wi-Fi"]',
         "Elegant hotel with world-class amenities in the business district.",
         4.3, 195),

        # Puri
        ("Toshali Sands", "Puri", "Ethnic Resort, Puri, Odisha 752001",
         19.8180, 85.8380, 5, 7800, 90,
         '["Beach Access", "Pool", "Spa", "Restaurant", "Ayurveda Center"]',
         "Beachfront ethnic resort with traditional Odishan architecture and Ayurveda.",
         4.8, 512),

        ("Mayfair Beach Resort Puri", "Puri", "C.T. Road, Chakratirtha, Puri 752002",
         19.8200, 85.8420, 5, 9500, 65,
         '["Private Beach", "Pool", "Spa", "Multiple Restaurants", "Kids Club"]',
         "Iconic beachfront luxury resort overlooking the Bay of Bengal.",
         4.9, 680),

        ("Hotel Hans Coco Palms", "Puri", "C.T. Road, Puri 752001",
         19.8215, 85.8440, 4, 4200, 50,
         '["Sea View Rooms", "Restaurant", "Bar", "Travel Desk"]',
         "Popular beach hotel with panoramic sea views and easy temple access.",
         4.1, 324),

        # Cuttack
        ("Mayfair Riverside", "Cuttack", "Mahanadi Riverside, Cuttack 753001",
         20.4625, 85.8830, 4, 5500, 55,
         '["River View", "Restaurant", "Conference", "Pool", "Bar"]',
         "Scenic riverside hotel with stunning views of the Mahanadi river.",
         4.4, 267),

        # Mumbai
        ("The Taj Mahal Palace", "Mumbai", "Apollo Bunder, Colaba, Mumbai 400001",
         18.9220, 72.8330, 5, 35000, 285,
         '["Heritage Architecture", "Fine Dining", "Spa", "Pool", "Butler Service", "Sea View"]',
         "India's most iconic luxury hotel, a symbol of Mumbai's grandeur since 1903.",
         4.9, 4521),

        ("ITC Grand Central", "Mumbai", "287, Dr. Babasaheb Ambedkar Rd, Parel, Mumbai 400012",
         19.0030, 72.8420, 5, 22000, 220,
         '["Pool", "Spa", "Multiple Restaurants", "Bar", "Business Center"]',
         "Luxury ITC hotel offering sustainable hospitality in central Mumbai.",
         4.7, 1876),

        ("The Leela Mumbai", "Mumbai", "Sahar, Andheri East, Mumbai 400059",
         19.1030, 72.8660, 5, 19500, 392,
         '["Pool", "Spa", "Fine Dining", "Butler Service", "Airport Proximity"]',
         "Opulent five-star retreat near international airport with Indian luxury.",
         4.6, 2103),

        # Delhi
        ("The Imperial New Delhi", "Delhi", "Janpath, New Delhi 110001",
         28.6235, 77.2195, 5, 28000, 234,
         '["Heritage Property", "Spa", "Fine Dining", "Pool", "Art Collection"]',
         "A landmark colonial-era luxury hotel with priceless art collection.",
         4.8, 3210),

        ("Shangri-La Eros New Delhi", "Delhi", "19, Ashoka Road, New Delhi 110001",
         28.6252, 77.2230, 5, 24000, 258,
         '["Pool", "Spa", "Multiple Restaurants", "Club Lounge", "Gym"]',
         "Luxurious contemporary hotel adjacent to Connaught Place.",
         4.7, 2870),

        # Goa
        ("Grand Hyatt Goa", "Goa", "Bambolim Bay Resort, Bambolim, Goa 403206",
         15.4820, 73.8820, 5, 21000, 302,
         '["Beach", "5 Pools", "Spa", "Multiple Dining", "Water Sports"]',
         "Sprawling resort on a pristine bay with world-class recreational facilities.",
         4.8, 3840),

        ("Taj Exotica Goa", "Goa", "Calwaddo, Benaulim, South Goa 403716",
         15.2530, 73.9380, 5, 32000, 140,
         '["Private Beach", "Spa", "Water Villa", "Fine Dining", "Golf"]',
         "Ultra-luxury resort amid 56 acres of landscaped tropical gardens.",
         4.9, 2910),

        # Jaipur
        ("Rambagh Palace", "Jaipur", "Bhawani Singh Road, Jaipur 302005",
         26.8960, 75.7960, 5, 45000, 78,
         '["Heritage Palace", "Polo", "Spa", "Fine Dining", "Peacocks"]',
         "Former residence of the Maharaja of Jaipur, now India's most romantic palace hotel.",
         5.0, 1980),

        ("ITC Rajputana", "Jaipur", "Palace Road, Jaipur 302006",
         26.9160, 75.7930, 5, 18000, 216,
         '["Heritage Architecture", "Pool", "Spa", "Fine Dining", "Yoga"]',
         "Grand Rajputana heritage hotel blending royal tradition with modern luxury.",
         4.7, 1645),

        # Bangalore
        ("The Leela Palace Bengaluru", "Bangalore", "23, Airport Road, Bengaluru 560008",
         12.9716, 77.6412, 5, 23000, 357,
         '["Pool", "Spa", "Multiple Restaurants", "Club Lounge", "Gym"]',
         "Inspired by the Mysore Palace, a bastion of South Indian royal heritage.",
         4.8, 2560),

        # Kolkata
        ("The Oberoi Grand Kolkata", "Kolkata", "15 Jawaharlal Nehru Road, Kolkata 700013",
         22.5626, 88.3526, 5, 19000, 209,
         '["Heritage Property", "Pool", "Spa", "Fine Dining", "Colonial Architecture"]',
         "A grande dame of Kolkata hospitality since 1930 with Victorian grandeur.",
         4.8, 1890),
    ]

    c.executemany("""
        INSERT INTO hotels (name, city, address, lat, lon, star_rating, base_price,
                           total_rooms, amenities, description, rating, review_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, hotels)

    # Seed reviews
    reviewers = ["Priya S.", "Rahul M.", "Ananya K.", "Vikram R.", "Sunita D.",
                 "James T.", "Maria G.", "Chen Wei", "Fatima A.", "David L."]
    comments = [
        "Absolutely stunning property! The service was impeccable.",
        "Perfect location with breathtaking views. Will definitely return.",
        "The room was spacious and the amenities top-notch.",
        "Great value for money. Staff was very helpful and courteous.",
        "The spa was exceptional. Best massage I've ever had.",
        "Beautiful property but could improve on food quality.",
        "Excellent hospitality. The breakfast spread was amazing.",
        "Location is unbeatable. Right in the heart of the city.",
        "Clean rooms, responsive staff, and great dining options.",
        "The pool area was stunning. Kids loved it!"
    ]

    for hotel_id in range(1, len(hotels) + 1):
        for _ in range(random.randint(3, 6)):
            c.execute("""
                INSERT INTO reviews (hotel_id, reviewer_name, rating, comment, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                hotel_id,
                random.choice(reviewers),
                round(random.uniform(3.5, 5.0), 1),
                random.choice(comments),
                (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
            ))

    # Seed price history (last 30 days)
    for hotel_id in range(1, len(hotels) + 1):
        hotel_base = hotels[hotel_id - 1][7]  # base_price
        for day_back in range(30, 0, -1):
            date = (datetime.now() - timedelta(days=day_back)).strftime('%Y-%m-%d')
            weather_s = random.uniform(0.3, 1.0)
            demand_s = random.uniform(0.2, 0.9)
            supply = random.uniform(0.15, 0.85)
            price = hotel_base * (0.85 + weather_s * 0.35) * (0.75 + demand_s * 0.6) * (1 + max(0, (0.3 - supply) * 1.8))
            c.execute("""
                INSERT INTO price_history (hotel_id, recorded_date, price, occupancy_rate, weather_score, demand_score)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (hotel_id, date, round(price, 2), round((1-supply)*100, 1), round(weather_s, 3), round(demand_s, 3)))

    conn.commit()
    conn.close()
    print(f"✅ Database initialized at {DB_PATH}")
    print(f"   → {len(hotels)} hotels seeded")
    print(f"   → Reviews and price history added")

if __name__ == '__main__':
    init_db()
