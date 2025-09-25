import pandas as pd
import numpy as np
from datetime import datetime
import os

class HRDataProcessor:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.df = None
        self.original_shape = None
        self.processing_log = []
        self.start_time = datetime.now()
    
    def load_data(self):
        """Load raw HR data"""
        print("Loading HR data...")
        self.df = pd.read_csv(self.input_path)
        self.original_shape = self.df.shape
        self.processing_log.append(f"Loaded data: {self.original_shape[0]} rows, {self.original_shape[1]} columns")
        print(f"   Loaded {self.original_shape[0]} employee records")
        return self
    
    def data_quality_assessment(self):
        """Assess data quality issues"""
        print("Assessing data quality...")
        
        # Check for duplicates
        duplicates = self.df.duplicated().sum()
        self.processing_log.append(f"Found {duplicates} duplicate rows")
        
        # Check for missing values
        missing_summary = self.df.isnull().sum()
        missing_cols = missing_summary[missing_summary > 0]
        
        if not missing_cols.empty:
            self.processing_log.append("Missing values found:")
            for col, count in missing_cols.items():
                self.processing_log.append(f"  - {col}: {count} missing ({count/len(self.df)*100:.1f}%)")
        
        print(f"   Found {duplicates} duplicates and missing values in {len(missing_cols)} columns")
        return self
    
    def remove_duplicates(self):
        """Remove duplicate records"""
        print("Removing duplicates...")
        initial_count = len(self.df)
        self.df = self.df.drop_duplicates()
        removed = initial_count - len(self.df)
        
        self.processing_log.append(f"Removed {removed} duplicate rows")
        print(f"   Removed {removed} duplicate records")
        return self
    
    def handle_missing_values(self):
        """Handle missing values with business logic"""
        print("Handling missing values...")
        
        # Handle missing MonthlyIncome - fill with median by JobRole
        if 'MonthlyIncome' in self.df.columns:
            missing_income = self.df['MonthlyIncome'].isnull().sum()
            if missing_income > 0:
                # Fill with median income by job role
                self.df['MonthlyIncome'] = self.df.groupby('JobRole')['MonthlyIncome'].transform(
                    lambda x: x.fillna(x.median())
                )
                self.processing_log.append(f"Filled {missing_income} missing MonthlyIncome values using job role median")
        
        # Handle missing JobSatisfaction - fill with most common value (mode)
        if 'JobSatisfaction' in self.df.columns:
            missing_satisfaction = self.df['JobSatisfaction'].isnull().sum()
            if missing_satisfaction > 0:
                mode_satisfaction = self.df['JobSatisfaction'].mode()[0]
                self.df['JobSatisfaction'].fillna(mode_satisfaction, inplace=True)
                self.processing_log.append(f"Filled {missing_satisfaction} missing JobSatisfaction values with mode ({mode_satisfaction})")
        
        print(f"   Missing values handled using business logic")
        return self
    
    def standardize_data(self):
        """Standardize data formats and values"""
        print("Standardizing data formats...")
        
        # Standardize Department names
        if 'Department' in self.df.columns:
            # Convert to proper case and fix inconsistencies
            dept_mapping = {
                'SALES': 'Sales',
                'sales': 'Sales',
                'RESEARCH & DEVELOPMENT': 'Research & Development',
                'research & development': 'Research & Development',
                'HUMAN RESOURCES': 'Human Resources',
                'human resources': 'Human Resources'
            }
            self.df['Department'] = self.df['Department'].replace(dept_mapping)
            self.processing_log.append("Standardized Department names")
        
        # Standardize Gender values
        if 'Gender' in self.df.columns:
            gender_mapping = {
                'M': 'Male',
                'F': 'Female',
                'm': 'Male',
                'f': 'Female'
            }
            self.df['Gender'] = self.df['Gender'].replace(gender_mapping)
            self.processing_log.append("Standardized Gender values")
        
        # Ensure MonthlyIncome is positive
        if 'MonthlyIncome' in self.df.columns:
            negative_incomes = (self.df['MonthlyIncome'] < 0).sum()
            if negative_incomes > 0:
                self.df['MonthlyIncome'] = self.df['MonthlyIncome'].abs()
                self.processing_log.append(f"Fixed {negative_incomes} negative income values")
        
        print("   Data formats standardized")
        return self
    
    def create_derived_features(self):
        """Create useful derived features"""
        print("Creating derived features...")
        
        # Create age groups
        if 'Age' in self.df.columns:
            self.df['AgeGroup'] = pd.cut(self.df['Age'], 
                                       bins=[0, 30, 40, 50, 100], 
                                       labels=['Under 30', '30-40', '40-50', '50+'])
            self.processing_log.append("Created AgeGroup categories")
        
        # Create income categories
        if 'MonthlyIncome' in self.df.columns:
            income_quartiles = self.df['MonthlyIncome'].quantile([0.25, 0.5, 0.75])
            self.df['IncomeCategory'] = pd.cut(self.df['MonthlyIncome'],
                                             bins=[0, income_quartiles[0.25], income_quartiles[0.5], 
                                                   income_quartiles[0.75], float('inf')],
                                             labels=['Low', 'Medium', 'High', 'Very High'])
            self.processing_log.append("Created IncomeCategory based on quartiles")
        
        print("   Derived features created")
        return self
    
    def save_processed_data(self):
        """Save the cleaned dataset"""
        print("Saving processed data...")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        
        # Save to CSV
        self.df.to_csv(self.output_path, index=False)
        self.processing_log.append(f"Cleaned data saved to {self.output_path}")
        
        # Also save as Excel with multiple sheets
        excel_path = self.output_path.replace('.csv', '.xlsx')
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            self.df.to_excel(writer, sheet_name='CleanData', index=False)
            
            # Create summary sheet
            summary_df = pd.DataFrame({
                'Metric': ['Total Records', 'Total Columns', 'Departments', 'Job Roles', 'Avg Age', 'Avg Monthly Income'],
                'Value': [
                    len(self.df),
                    len(self.df.columns),
                    self.df['Department'].nunique() if 'Department' in self.df.columns else 'N/A',
                    self.df['JobRole'].nunique() if 'JobRole' in self.df.columns else 'N/A',
                    f"{self.df['Age'].mean():.1f}" if 'Age' in self.df.columns else 'N/A',
                    f"${self.df['MonthlyIncome'].mean():,.0f}" if 'MonthlyIncome' in self.df.columns else 'N/A'
                ]
            })
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f"   Saved clean data ({len(self.df)} rows) to CSV and Excel")
        return self
    
    def generate_processing_report(self):
        """Generate comprehensive processing report"""
        print("Generating processing report...")
        
        end_time = datetime.now()
        processing_time = (end_time - self.start_time).total_seconds()
        
        report_path = 'reports/hr_data_processing_report.txt'
        os.makedirs('reports', exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write("HR DATA PROCESSING AUTOMATION REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Processing Date: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Processing Time: {processing_time:.2f} seconds\n")
            f.write(f"Input File: {self.input_path}\n")
            f.write(f"Output File: {self.output_path}\n\n")
            
            f.write("DATA TRANSFORMATION SUMMARY:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Original Shape: {self.original_shape[0]} rows × {self.original_shape[1]} columns\n")
            f.write(f"Final Shape: {self.df.shape[0]} rows × {self.df.shape[1]} columns\n")
            f.write(f"Rows Processed: {self.original_shape[0]}\n")
            f.write(f"Rows Retained: {self.df.shape[0]}\n")
            f.write(f"Data Quality Improvement: {((self.df.shape[0]/self.original_shape[0])*100):.1f}% clean data retention\n\n")
            
            f.write("PROCESSING STEPS COMPLETED:\n")
            f.write("-" * 30 + "\n")
            for i, step in enumerate(self.processing_log, 1):
                f.write(f"{i}. {step}\n")
            f.write("\n")
            
            # Add data insights
            if 'Department' in self.df.columns:
                f.write("DEPARTMENT DISTRIBUTION:\n")
                f.write("-" * 25 + "\n")
                dept_counts = self.df['Department'].value_counts()
                for dept, count in dept_counts.items():
                    f.write(f"• {dept}: {count} employees ({count/len(self.df)*100:.1f}%)\n")
                f.write("\n")
            
            if 'MonthlyIncome' in self.df.columns:
                f.write("SALARY STATISTICS:\n")
                f.write("-" * 18 + "\n")
                f.write(f"• Average Salary: ${self.df['MonthlyIncome'].mean():,.2f}\n")
                f.write(f"• Median Salary: ${self.df['MonthlyIncome'].median():,.2f}\n")
                f.write(f"• Salary Range: ${self.df['MonthlyIncome'].min():,.2f} - ${self.df['MonthlyIncome'].max():,.2f}\n")
                f.write(f"• Standard Deviation: ${self.df['MonthlyIncome'].std():,.2f}\n\n")
            
            f.write("EFFICIENCY METRICS:\n")
            f.write("-" * 20 + "\n")
            f.write(f"• Processing Speed: {self.original_shape[0]/processing_time:.0f} records/second\n")
            f.write(f"• Time Savings vs Manual: ~{(2*3600-processing_time)/3600:.1f} hours saved\n")
            f.write(f"• Estimated Manual Time: ~2 hours\n")
            f.write(f"• Automated Time: {processing_time:.1f} seconds\n")
            f.write(f"• Efficiency Gain: {((2*3600)/processing_time):.0f}x faster\n")
        
        print(f"   Comprehensive report saved to {report_path}")
        return self

def main():
    """Main execution function"""
    print("Starting HR Data Processing Automation...")
    print("-" * 50)
    
    # Initialize processor
    processor = HRDataProcessor(
        input_path='data/raw/hr_data_raw.csv',
        output_path='data/processed/hr_data_clean.csv'
    )
    
    # Execute processing pipeline
    processor.load_data() \
             .data_quality_assessment() \
             .remove_duplicates() \
             .handle_missing_values() \
             .standardize_data() \
             .create_derived_features() \
             .save_processed_data() \
             .generate_processing_report()
    
    print("-" * 50)
    print("HR Data Processing Complete!")
    print("\nOutputs generated:")
    print("• Clean CSV: data/processed/hr_data_clean.csv")
    print("• Excel file: data/processed/hr_data_clean.xlsx")
    print("• Processing report: reports/hr_data_processing_report.txt")

if __name__ == "__main__":
    main()
