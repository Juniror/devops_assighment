from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models
class BookBase(BaseModel):
    title: str
    author: str
    isbn: str
    year: int
    price: float

class BookCreate(BookBase):
    pass

class Book(BookBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class ErrorResponse(BaseModel):
    message: str

class HealthResponse(BaseModel):
    message: str
    error: Optional[str] = None

# FastAPI app
app = FastAPI(
    title="Simple API Example",
    description="This is a simple example of using FastAPI with Swagger.",
    version="1.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection pool
db_pool = None

def get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)

def init_db():
    global db_pool

    host = get_env("DB_HOST", "localhost")
    name = get_env("DB_NAME", "bookstore")
    user = get_env("DB_USER", "bookstore_user")
    password = get_env("DB_PASSWORD", "bookstore_password")
    port = get_env("DB_PORT", "5432")

    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            25,  # maxconn
            host=host,
            port=port,
            user=user,
            password=password,
            database=name
        )

        # Test connection
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        db_pool.putconn(conn)

        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def get_db_connection():
    if db_pool is None:
        raise Exception("Database pool not initialized")
    return db_pool.getconn()

def return_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)

@app.on_event("startup")
async def startup_event():
    init_db()

@app.on_event("shutdown")
async def shutdown_event():
    if db_pool:
        db_pool.closeall()
        logger.info("Database connections closed")

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return_db_connection(conn)
        return {"message": "healthy"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "unhealthy", "error": str(e)}
        )

@app.get("/api/v1/books", response_model=List[Book], tags=["Books"])
async def get_all_books():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT id, title, author, isbn, year, price, created_at, updated_at
            FROM books
            ORDER BY id
        """)

        books = cursor.fetchall()
        cursor.close()
        return_db_connection(conn)

        return books if books else []

    except Exception as e:
        if conn:
            return_db_connection(conn)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/api/v1/books/{book_id}", response_model=Book, tags=["Books"])
async def get_book(book_id: int):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT id, title, author, isbn, year, price, created_at, updated_at
            FROM books
            WHERE id = %s
        """, (book_id,))

        book = cursor.fetchone()
        cursor.close()
        return_db_connection(conn)

        if book is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="book not found"
            )

        return book

    except HTTPException:
        if conn:
            return_db_connection(conn)
        raise
    except Exception as e:
        if conn:
            return_db_connection(conn)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/api/v1/books", response_model=Book, status_code=status.HTTP_201_CREATED, tags=["Books"])
async def create_book(book: BookCreate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            INSERT INTO books (title, author, isbn, year, price)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, title, author, isbn, year, price, created_at, updated_at
        """, (book.title, book.author, book.isbn, book.year, book.price))

        new_book = cursor.fetchone()
        conn.commit()
        cursor.close()
        return_db_connection(conn)

        return new_book

    except Exception as e:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.put("/api/v1/books/{book_id}", response_model=Book, tags=["Books"])
async def update_book(book_id: int, book: BookCreate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            UPDATE books
            SET title = %s, author = %s, isbn = %s, year = %s, price = %s
            WHERE id = %s
            RETURNING id, title, author, isbn, year, price, created_at, updated_at
        """, (book.title, book.author, book.isbn, book.year, book.price, book_id))

        updated_book = cursor.fetchone()

        if updated_book is None:
            cursor.close()
            return_db_connection(conn)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="book not found"
            )

        conn.commit()
        cursor.close()
        return_db_connection(conn)

        return updated_book

    except HTTPException:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.delete("/api/v1/books/{book_id}", tags=["Books"])
async def delete_book(book_id: int):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM books WHERE id = %s", (book_id,))
        rows_affected = cursor.rowcount

        if rows_affected == 0:
            cursor.close()
            return_db_connection(conn)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="book not found"
            )

        conn.commit()
        cursor.close()
        return_db_connection(conn)

        return {"message": "book deleted successfully"}

    except HTTPException:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)