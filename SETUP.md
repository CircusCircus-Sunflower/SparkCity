# SparkCity Setup Guide

## Prerequisites
- Python 3.8+
- Java 11
- Git

## Setup Steps

1. **Clone the repository:**
```bash
   git clone https://github.com/CircusCircus-Sunflower/SparkCity.git
   cd SparkCity
```

2. **Create virtual environment:**
```bash
   python3 -m venv venv
   source venv/bin/activate
```

3. **Install dependencies:**
```bash
   pip install -r requirements.txt
```

4. **Configure Java:**
```bash
   export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
   export PATH="$JAVA_HOME/bin:$PATH"
```

5. **Test setup:**
```bash
   python src/data_quality/data_quality_pipeline.py
```

## Daily Usage
```bash
# Start work
cd SparkCity
source venv/bin/activate

# Do your work...

# End work
deactivate
```
