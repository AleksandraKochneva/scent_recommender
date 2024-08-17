# Perfume Recommendation App

This is a Flask-based web application that predicts whether a user will like a perfume based on historical data and machine learning predictions. The app allows users to vote on perfumes, check predictions, and add new perfumes to the catalog.
![App Interface](image.png)

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Routes](#routes)
- [Modules](#modules)
  - [db.py](#dbpy)
  - [parser.py](#parserpy)
  - [get_prediction.py](#get_predictionpy)
  - [main.py](#mainpy)
  - [model_training.py](#model_trainingpy)
- [Contributing](#contributing)
- [License](#license)

## Features

- View the perfume catalog
- Predict if a user will like a perfume based on a machine learning model
- Vote for perfumes you like or dislike
- Add new perfumes to the catalog
- Background model training using threading
- Web scraping to retrieve perfume data and brand information

## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/username/perfume-recommendation-app.git
    cd perfume-recommendation-app
    ```

2. **Set up a virtual environment (optional but recommended)**:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install the required dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up the environment variables**:
   - Create a `.env` file in the root directory and add the following:
     ```
     secret_key=your_secret_key_here
     c_string=your_database_connection_string
     ca_string=your_another_connection_string
     proxy_url=your_proxy_url
     m_string=your_mongo_connection_string
     main_url=main_website_url
     ```

5. **Initialize the database** (if required by the `db` module):
   - Ensure your database is properly set up and configured.

## Usage

1. **Run the application**:
    ```bash
    python app.py
    ```

2. **Access the app**:
   - Open your browser and go to `http://127.0.0.1:5000/`.

3. **Interacting with the app**:
   - Browse the catalog and select a perfume to check predictions.
   - Vote for your favorite perfumes.
   - Add new perfumes to the catalog.

## Routes

- **`/`**: Home page showing the perfume catalog.
- **`/check/<perfume_name>`**: Check if the selected perfume will be liked based on predictions.
- **`/vote`**: Vote for perfumes (like or dislike).
- **`/add`**: Add new perfumes to the catalog.

## Modules

### db.py

The `db.py` module handles database operations using SQLAlchemy and PostgreSQL. It provides functions for inserting data, retrieving datasets, and managing database interactions.

### parser.py

The `parsers.py` module handles web scraping and data extraction from external sources. It uses `requests`, `BeautifulSoup`, and `cloudscraper` to gather information about perfumes, brands, and reviews.

### get_prediction.py

The `get_prediction.py` module is responsible for preparing data and generating predictions based on machine learning models. It uses MongoDB for model storage and retrieval, and leverages `pandas`, `joblib`, and `gridfs` for data preprocessing and model application.

### main.py

The `main.py` module orchestrates the updating of perfume data and the addition of new entries to the catalog. It interacts with web scraping and database insertion functions to ensure the latest data is available.

### model_training.py

The `model_training.py` module handles the training of machine learning models for predicting perfume preferences. It prepares data, trains Random Forest and XGBoost models, and saves the trained models to MongoDB.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/AmazingFeature`).
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4. Push to the branch (`git push origin feature/AmazingFeature`).
5. Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
