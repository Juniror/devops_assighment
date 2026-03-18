from calculator import add, subtract, multiply, divide

def main():
    """Run calculator demo."""
    print("=== Calculator App ===")
    print(f"10 + 5 = {add(10, 5)}")
    print(f"10 - 5 = {subtract(10, 5)}")
    print(f"10 * 5 = {multiply(10, 5)}")
    print(f"10 / 5 = {divide(10, 5)}")
    print("\nCalculator is working!")


if __name__ == "__main__":
    main()