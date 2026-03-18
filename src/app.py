"""Main application entry point for the calculator."""

from calculator import add, subtract, multiply, divide


def main():
    """Main function to run the calculator application."""
    print("Calculator Application")
    print("=" * 40)
    
    # Example calculations
    a, b = 10, 5
    
    print(f"a = {a}, b = {b}")
    print(f"add({a}, {b}) = {add(a, b)}")
    print(f"subtract({a}, {b}) = {subtract(a, b)}")
    print(f"multiply({a}, {b}) = {multiply(a, b)}")
    print(f"divide({a}, {b}) = {divide(a, b)}")


if __name__ == "__main__":
    main()
