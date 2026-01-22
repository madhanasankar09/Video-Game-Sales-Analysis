import pandas as pd

import pandas as pd

games = pd.read_csv(r"D:\AI & ML\Video Game project\games.csv")
sales = pd.read_csv(r"D:\AI & ML\Video Game project\vgsales.csv")

print(games.head())
print(sales.head())

# Show column names
print("\nGames Columns:\n", games.columns)
print("\nSales Columns:\n", sales.columns)

# Check missing values
print("\nMissing values in games:\n", games.isnull().sum())
print("\nMissing values in sales:\n", sales.isnull().sum())

# Remove duplicate rows
games = games.drop_duplicates()
sales = sales.drop_duplicates()

print("\nAfter removing duplicates:")
print("Games shape:", games.shape)
print("Sales shape:", sales.shape)

# Fill missing numeric values
if 'Rating' in games.columns:
    games['Rating'] = games['Rating'].fillna(games['Rating'].mean())

for col in ['Playing', 'Backlogs', 'Wishlist']:
    if col in games.columns:
        games[col] = games[col].fillna(0)

# Fill missing text values
for col in ['Title', 'Genres', 'Platform', 'Team']:
    if col in games.columns:
        games[col] = games[col].fillna("unknown")

for col in ['Name', 'Platform', 'Genre', 'Publisher']:
    if col in sales.columns:
        sales[col] = sales[col].fillna("unknown")

# Verify missing values after cleaning
print("\nMissing values in games after cleaning:\n", games.isnull().sum())
print("\nMissing values in sales after cleaning:\n", sales.isnull().sum())

# Handle missing Release Date in games
if 'Release Date' in games.columns:
    games['Release Date'] = games['Release Date'].fillna("unknown")

# Handle missing Year in sales
if 'Year' in sales.columns:
    # Fill with median year (safer than mean for years)
    sales['Year'] = sales['Year'].fillna(sales['Year'].median())

print("\nMissing values in games after final cleaning:\n", games.isnull().sum())
print("\nMissing values in sales after final cleaning:\n", sales.isnull().sum())

# Find exactly which column still has missing value in games
print("\nColumns in games that still have missing values:")
print(games.isnull().sum()[games.isnull().sum() > 0])

# Fix missing value in Summary column
if 'Summary' in games.columns:
    games['Summary'] = games['Summary'].fillna("unknown")

print("\nMissing values in games after fixing Summary:\n", games.isnull().sum())


# Normalize text columns in games dataset
text_cols_games = ['Title', 'Genres', 'Platform', 'Team', 'Summary']

for col in text_cols_games:
    if col in games.columns:
        games[col] = games[col].astype(str).str.lower().str.strip()

# Normalize text columns in sales dataset
text_cols_sales = ['Name', 'Platform', 'Genre', 'Publisher']

for col in text_cols_sales:
    if col in sales.columns:
        sales[col] = sales[col].astype(str).str.lower().str.strip()

# Show only columns that actually exist
existing_game_cols = [col for col in text_cols_games if col in games.columns]
existing_sales_cols = [col for col in text_cols_sales if col in sales.columns]

print(games[existing_game_cols].head())
print(sales[existing_sales_cols].head())


# Standardize date format in games
if 'Release Date' in games.columns:
    games['Release Date'] = pd.to_datetime(games['Release Date'], errors='coerce')

# Ensure numeric columns are numeric in games
for col in ['Rating', 'Playing', 'Backlogs', 'Wishlist']:
    if col in games.columns:
        games[col] = pd.to_numeric(games[col], errors='coerce').fillna(0)

# Ensure numeric columns are numeric in sales
for col in ['Year', 'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']:
    if col in sales.columns:
        sales[col] = pd.to_numeric(sales[col], errors='coerce').fillna(0)

# Save cleaned datasets
games.to_csv("games_clean.csv", index=False)
sales.to_csv("vgsales_clean.csv", index=False)

print("\nCleaned files saved as games_clean.csv and vgsales_clean.csv")

import sqlite3

# Create / connect to SQLite database
conn = sqlite3.connect("videogame.db")
cursor = conn.cursor()

print("Database connected successfully.")

# Insert cleaned data into SQL tables
games.to_sql("games_engagement", conn, if_exists="replace", index=False)
sales.to_sql("games_sales", conn, if_exists="replace", index=False)

print("Tables games_engagement and games_sales created.")


merge_query = """
CREATE TABLE IF NOT EXISTS merged_games AS
SELECT 
    g.Title,
    g.Rating,
    g.Genres,
    g.Playing,
    g.Backlogs,
    g.Wishlist,
    g."Release Date",
    g.Team,
    g.Summary,
    s.Platform AS sales_platform,
    s.Genre AS sales_genre,
    s.Publisher,
    s.NA_Sales,
    s.EU_Sales,
    s.JP_Sales,
    s.Other_Sales,
    s.Global_Sales,
    s.Year
FROM games_engagement g
JOIN games_sales s
ON g.Title = s.Name;
"""


cursor.execute("DROP TABLE IF EXISTS merged_games;")
cursor.execute(merge_query)
conn.commit()

print("Merged table created successfully.")

# Check row counts
print("\nGames Engagement rows:",
      cursor.execute("SELECT COUNT(*) FROM games_engagement").fetchone()[0])

print("Games Sales rows:",
      cursor.execute("SELECT COUNT(*) FROM games_sales").fetchone()[0])

print("Merged rows:",
      cursor.execute("SELECT COUNT(*) FROM merged_games").fetchone()[0])

print("\n================ EDA QUERIES ================\n")

# 1. Top 10 Rated Games
q1 = """
SELECT Title, Rating
FROM games_engagement
ORDER BY Rating DESC
LIMIT 10;
"""
print("Top 10 Rated Games:")
for r in cursor.execute(q1).fetchall():
    print(r)

# 2. Most Common Genres
q2 = """
SELECT Genres, COUNT(*) AS cnt
FROM games_engagement
GROUP BY Genres
ORDER BY cnt DESC
LIMIT 10;
"""
print("\nMost Common Genres:")
for r in cursor.execute(q2).fetchall():
    print(r)

# 3. Top 10 Most Wishlisted Games
q3 = """
SELECT Title, Wishlist
FROM games_engagement
ORDER BY Wishlist DESC
LIMIT 10;
"""
print("\nTop 10 Most Wishlisted Games:")
for r in cursor.execute(q3).fetchall():
    print(r)

# 4. Best Selling Platforms
q4 = """
SELECT Platform, SUM(Global_Sales) AS total_sales
FROM games_sales
GROUP BY Platform
ORDER BY total_sales DESC
LIMIT 10;
"""
print("\nBest Selling Platforms:")
for r in cursor.execute(q4).fetchall():
    print(r)

# 5. Region with Highest Sales
q5 = """
SELECT 
    SUM(NA_Sales) AS NA,
    SUM(EU_Sales) AS EU,
    SUM(JP_Sales) AS JP,
    SUM(Other_Sales) AS Other
FROM games_sales;
"""
print("\nRegion-wise Total Sales:")
print(cursor.execute(q5).fetchone())

# 6. Top 10 Best-Selling Games Globally
q6 = """
SELECT Name, Global_Sales
FROM games_sales
ORDER BY Global_Sales DESC
LIMIT 10;
"""
print("\nTop 10 Best-Selling Games:")
for r in cursor.execute(q6).fetchall():
    print(r)

# 7. Genres by Global Sales (Merged)
q7 = """
SELECT sales_genre, SUM(Global_Sales) AS total_sales
FROM merged_games
GROUP BY sales_genre
ORDER BY total_sales DESC
LIMIT 10;
"""
print("\nGenres by Global Sales:")
for r in cursor.execute(q7).fetchall():
    print(r)

# 8. Rating vs Sales (sample)
q8 = """
SELECT Rating, Global_Sales
FROM merged_games
WHERE Rating IS NOT NULL
LIMIT 10;
"""
print("\nRating vs Sales (Sample):")
for r in cursor.execute(q8).fetchall():
    print(r)

# 9. Do Highly Wishlisted Games Sell More?
q9 = """
SELECT 
    AVG(Global_Sales) AS avg_sales,
    AVG(Wishlist) AS avg_wishlist
FROM merged_games;
"""
print("\nAverage Sales vs Wishlist:")
print(cursor.execute(q9).fetchone())

# 10. Top Publishers by Sales
q10 = """
SELECT Publisher, SUM(Global_Sales) AS total_sales
FROM games_sales
GROUP BY Publisher
ORDER BY total_sales DESC
LIMIT 10;
"""
print("\nTop Publishers by Sales:")
for r in cursor.execute(q10).fetchall():
    print(r)
