"""
Mock tests for Lakehouse Engine DAGs
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestLakehouseDAGs(unittest.TestCase):
    """Test cases for Lakehouse Engine DAGs"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_data_dir = "/tmp/test_data_files"
        os.makedirs(self.test_data_dir, exist_ok=True)
    
    def tearDown(self):
        """Clean up after tests"""
        import shutil
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    @patch('lakehouse.jobs.run_finance_stocks_etl')
    def test_finance_dag_load(self, mock_etl):
        """Test finance DAG loads data correctly"""
        mock_etl.return_value = {"status": "success", "rows": 100}
        
        from dags.lakehouse_finance_dag import load_finance_to_delta
        
        result = load_finance_to_delta()
        
        self.assertIsNotNone(result)
        mock_etl.assert_called_once()
    
    @patch('lakehouse.jobs.run_economics_etl')
    def test_economics_dag_load(self, mock_etl):
        """Test economics DAG loads data correctly"""
        mock_etl.return_value = {"status": "success", "rows": 50}
        
        from dags.lakehouse_economics_dag import load_economics_to_delta
        
        result = load_economics_to_delta()
        
        self.assertIsNotNone(result)
        mock_etl.assert_called_once()
    
    @patch('os.path.exists')
    @patch('os.listdir')
    def test_check_data_files(self, mock_listdir, mock_exists):
        """Test data file checking"""
        mock_exists.return_value = True
        mock_listdir.return_value = ['file1.json', 'file2.json', 'summary.json']
        
        from dags.lakehouse_finance_dag import check_data_files
        
        files = check_data_files()
        
        self.assertEqual(len(files), 3)
        self.assertIn('file1.json', files)
    
    @patch('pyspark.sql.SparkSession')
    @patch('os.path.exists')
    def test_verify_delta_table(self, mock_exists, mock_spark_session):
        """Test Delta table verification"""
        mock_exists.return_value = True
        
        # Mock Spark DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.show.return_value = None
        
        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        from dags.lakehouse_finance_dag import verify_delta_table
        
        with patch('dags.lakehouse_finance_dag.SparkSession', mock_spark_session):
            count = verify_delta_table()
        
        self.assertEqual(count, 100)
        mock_df.count.assert_called_once()


class TestMockDataGeneration(unittest.TestCase):
    """Test mock data generation utilities"""
    
    def test_create_mock_json_file(self):
        """Test creating mock JSON files for testing"""
        import json
        import tempfile
        
        test_data = {
            "symbol": "AAPL",
            "data": [{"date": "2024-01-01", "price": 150.0}],
            "fetched_at": "2024-01-01T00:00:00"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_data, f)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                loaded_data = json.load(f)
            
            self.assertEqual(loaded_data['symbol'], 'AAPL')
            self.assertIn('data', loaded_data)
        finally:
            os.unlink(temp_path)


if __name__ == '__main__':
    unittest.main()

