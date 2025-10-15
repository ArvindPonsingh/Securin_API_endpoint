from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

# Database connection
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Local_db"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# Helper function to parse operators
def parse_filter(value):
    if not value:
        return None, None
    for op in ['>=', '<=', '>', '<', '=']:
        if value.startswith(op):
            return op, value[len(op):]
    return '=', value

# ✅ Endpoint: Get paginated recipes
@app.route("/api/recipes", methods=["GET"])
def get_recipes():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))
    offset = (page - 1) * limit

    with SessionLocal() as session:
        total = session.execute(text("SELECT COUNT(*) FROM recipes")).scalar()
        result = session.execute(
            text("SELECT * FROM recipes ORDER BY rating DESC NULLS LAST LIMIT :limit OFFSET :offset"),
            {"limit": limit, "offset": offset}
        )
        data = [dict(row._mapping) for row in result]

    return jsonify({
        "page": page,
        "limit": limit,
        "total": total,
        "data": data
    })

# ✅ Endpoint: Search recipes with filters
@app.route("/api/recipes/search", methods=["GET"])
def search_recipes():
    calories = request.args.get("calories")
    title = request.args.get("title")
    cuisine = request.args.get("cuisine")
    total_time = request.args.get("total_time")
    rating = request.args.get("rating")

    filters = []
    params = {}

    if calories:
        op, val = parse_filter(calories)
        filters.append("(nutrients->>'calories')~'^[0-9]+' AND (nutrients->>'calories')::int {} :calories".format(op))
        params["calories"] = int(val)

    if title:
        filters.append("title ILIKE :title")
        params["title"] = f"%{title}%"

    if cuisine:
        filters.append("cuisine = :cuisine")
        params["cuisine"] = cuisine

    if total_time:
        op, val = parse_filter(total_time)
        filters.append(f"total_time {op} :total_time")
        params["total_time"] = int(val)

    if rating:
        op, val = parse_filter(rating)
        filters.append(f"rating {op} :rating")
        params["rating"] = float(val)

    where_clause = " AND ".join(filters) if filters else "TRUE"

    query = f"SELECT * FROM recipes WHERE {where_clause} ORDER BY rating DESC NULLS LAST"

    with SessionLocal() as session:
        result = session.execute(text(query), params)
        data = [dict(row._mapping) for row in result]

    return jsonify({"data": data})

if __name__ == "__main__":
    app.run(debug=True)