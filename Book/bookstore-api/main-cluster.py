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
    write_host: Optional[str] = None
    read_host: Optional[str] = None

# FastAPI app
app = FastAPI(
    title="Bookstore API with PostgreSQL Cluster",
    description="FastAPI with read/write splitting for PostgreSQL Cluster",
    version="2.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection pools
write_pool = None
read_pool = None

def get_env(key: str, default: str = "") -> str:
    """Get environment variable with default value"""
    return os.getenv(key, default)

def init_db():
    """Initialize database connection pools for write and read"""
    global write_pool, read_pool

    # Write connection configuration (Primary)
    write_host = get_env("DB_WRITE_HOST", "bookstore-postgres-cluster.dev.svc.cluster.local")

    # Read connection configuration (Replicas)
    read_host = get_env("DB_READ_HOST", "bookstore-postgres-cluster-repl.dev.svc.cluster.local")

    # Common database configuration
    db_name = get_env("DB_NAME", "bookstore")
    db_user = get_env("DB_USER", "bookstore_user")
    db_password = get_env("DB_PASSWORD", "bookstore_password")
    db_port = get_env("DB_PORT", "5432")

    try:
        # Create write connection pool (Primary)
        write_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            25,  # maxconn
            host=write_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            sslmode='require'
        )

        # Test write connection
        conn = write_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        write_pool.putconn(conn)

        logger.info(f"Successfully connected to write database at {write_host}")

        # Create read connection pool (Replicas)
        read_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            25,  # maxconn
            host=read_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            sslmode='require'
        )

        # Test read connection
        conn = read_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        read_pool.putconn(conn)

        logger.info(f"Successfully connected to read database at {read_host}")

    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def get_write_connection():
    """Get connection for write operations (Primary)"""
    if write_pool is None:
        raise Exception("Write pool not initialized")
    return write_pool.getconn()

def get_read_connection():
    """Get connection for read operations (Replicas)"""
    if read_pool is None:
        raise Exception("Read pool not initialized")
    return read_pool.getconn()

def return_write_connection(conn):
    """Return connection to write pool"""
    if write_pool:
        write_pool.putconn(conn)

def return_read_connection(conn):
    """Return connection to read pool"""
    if read_pool:
        read_pool.putconn(conn)

@app.on_event("startup")
async def startup_event():
    """Initialize database connections on startup"""
    init_db()
    logger.info("API started with read/write splitting enabled")

@app.on_event("shutdown")
async def shutdown_event():
    """Close all database connections on shutdown"""
    if write_pool:
        write_pool.closeall()
        logger.info("Write database connections closed")
    if read_pool:
        read_pool.closeall()
        logger.info("Read database connections closed")

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint
    Tests connectivity to both write and read databases
    """
    write_status = "unknown"
    read_status = "unknown"

    try:
        # Test write connection
        conn = get_write_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return_write_connection(conn)
        write_status = "healthy"

        # Test read connection
        conn = get_read_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return_read_connection(conn)
        read_status = "healthy"

        return {
            "message": "healthy",
            "write_host": get_env("DB_WRITE_HOST", "unknown"),
            "read_host": get_env("DB_READ_HOST", "unknown")
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "unhealthy",
                "error": str(e),
                "write_status": write_status,
                "read_status": read_status
            }
        )

@app.get("/api/v1/books", response_model=List[Book], tags=["Books"])
async def get_all_books():
    """
    Get all books
    Uses READ connection (Replicas)
    """
    conn = None
    try:
        # Use read connection for SELECT queries
        conn = get_read_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT id, title, author, isbn, year, price, created_at, updated_at
            FROM books
            ORDER BY id
        """)

        books = cursor.fetchall()
        cursor.close()
        return_read_connection(conn)

        logger.info(f"Retrieved {len(books)} books from read replica")
        return books if books else []

    except Exception as e:
        if conn:
            return_read_connection(conn)
        logger.error(f"Error getting all books: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/api/v1/books/{book_id}", response_model=Book, tags=["Books"])
async def get_book(book_id: int):
    """
    Get a specific book by ID
    Uses READ connection (Replicas)
    """
    conn = None
    try:
        # Use read connection for SELECT queries
        conn = get_read_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT id, title, author, isbn, year, price, created_at, updated_at
            FROM books
            WHERE id = %s
        """, (book_id,))

        book = cursor.fetchone()
        cursor.close()
        return_read_connection(conn)

        if book is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="book not found"
            )

        logger.info(f"Retrieved book {book_id} from read replica")
        return book

    except HTTPException:
        if conn:
            return_read_connection(conn)
        raise
    except Exception as e:
        if conn:
            return_read_connection(conn)
        logger.error(f"Error getting book {book_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/api/v1/books", response_model=Book, status_code=status.HTTP_201_CREATED, tags=["Books"])
async def create_book(book: BookCreate):
    """
    Create a new book
    Uses WRITE connection (Primary)
    """
    conn = None
    try:
        # Use write connection for INSERT queries
        conn = get_write_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            INSERT INTO books (title, author, isbn, year, price)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, title, author, isbn, year, price, created_at, updated_at
        """, (book.title, book.author, book.isbn, book.year, book.price))

        new_book = cursor.fetchone()
        conn.commit()
        cursor.close()
        return_write_connection(conn)

        logger.info(f"Created new book {new_book['id']} on primary")
        return new_book

    except Exception as e:
        if conn:
            conn.rollback()
            return_write_connection(conn)
        logger.error(f"Error creating book: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.put("/api/v1/books/{book_id}", response_model=Book, tags=["Books"])
async def update_book(book_id: int, book: BookCreate):
    """
    Update an existing book
    Uses WRITE connection (Primary)
    """
    conn = None
    try:
        # Use write connection for UPDATE queries
        conn = get_write_connection()
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
            return_write_connection(conn)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="book not found"
            )

        conn.commit()
        cursor.close()
        return_write_connection(conn)

        logger.info(f"Updated book {book_id} on primary")
        return updated_book

    except HTTPException:
        if conn:
            conn.rollback()
            return_write_connection(conn)
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            return_write_connection(conn)
        logger.error(f"Error updating book {book_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.delete("/api/v1/books/{book_id}", tags=["Books"])
async def delete_book(book_id: int):
    """
    Delete a book
    Uses WRITE connection (Primary)
    """
    conn = None
    try:
        # Use write connection for DELETE queries
        conn = get_write_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM books WHERE id = %s", (book_id,))
        rows_affected = cursor.rowcount

        if rows_affected == 0:
            cursor.close()
            return_write_connection(conn)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="book not found"
            )

        conn.commit()
        cursor.close()
        return_write_connection(conn)

        logger.info(f"Deleted book {book_id} on primary")
        return {"message": "book deleted successfully"}

    except HTTPException:
        if conn:
            conn.rollback()
            return_write_connection(conn)
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            return_write_connection(conn)
        logger.error(f"Error deleting book {book_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/api/v1/stats", tags=["Statistics"])
async def get_stats():
    """
    Get database statistics
    Uses READ connection (Replicas)
    """
    conn = None
    try:
        conn = get_read_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get book statistics
        cursor.execute("""
            SELECT
                COUNT(*) as total_books,
                COUNT(DISTINCT author) as total_authors,
                MIN(year) as oldest_year,
                MAX(year) as newest_year,
                AVG(price) as average_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM books
        """)

        stats = cursor.fetchone()
        cursor.close()
        return_read_connection(conn)

        logger.info("Retrieved statistics from read replica")
        return stats

    except Exception as e:
        if conn:
            return_read_connection(conn)
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)