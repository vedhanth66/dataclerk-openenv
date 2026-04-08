from fastapi import FastAPI

def main():
    app = FastAPI()

    @app.get("/")
    def root():
        return {"status": "ok"}

    return app


if __name__ == "__main__":
    main()