# Objectif : enchaîner les 4 loaders

import subprocess
import sys

def run(module: str):
    # 1) Appeler un module Python sous la forme "python -m <module>"
    print(f"\n=== Running {module} ===")
    subprocess.run([sys.executable, "-m", module], check=True)


def main():
    # 2) Enchaîner les loaders dans l'ordre logique
    run("loaders.load_products")
    run("loaders.load_customers")
    run("loaders.load_sales_customers")
    run("loaders.load_sales_products")


if __name__ == "__main__":
    main()
