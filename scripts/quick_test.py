import pandas as pd

def quick_verification():
    """Quick verification that processing worked"""
    print("Quick Data Verification...")
    
    # Load processed data
    df = pd.read_csv('data/processed/hr_data_clean.csv')
    
    print(f"Processed data loaded: {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    print(f"No duplicates: {df.duplicated().sum() == 0}")
    print(f"Missing values handled: {df.isnull().sum().sum()} total missing")
    
    print("\nSample of clean data:")
    print(df.head(3).to_string())
    
    return df

if __name__ == "__main__":
    quick_verification()
