# Solo puedes importar las siguientes librerías y ninguna otra
import sys
# Librerías adicionales por si necesitas ocuparlas. No son esperadas, pero puedes usarlas si quieres.
import typing, os, pathlib, math, re, collections

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 main.py [PATH_TO_INPUT_FILE]")
        sys.exit(1)

    print(sys.argv)
