import pytest
import allure
import pandas as pd
import re
from typing import Dict, List, Any
from utils.fabric_client import FabricClient
from utils.predefined_validations import PredefinedValidations

@allure.epic("ETL Testing Framework")
@allure.feature("CSV-Driven ETL Validation")
class TestCSVDrivenETLValidation:
    """CSV-Driven ETL Validation - Manual testers control tests via CSV"""
    
    @classmethod
    def setup_class(cls):
        """Setup database clients and load CSV tests"""
        cls.source_client = FabricClient('BRONZE')
        cls.target_client = FabricClient('SILVER')
        cls.validator = PredefinedValidations()
        cls.test_cases = cls._load_test_cases()
    
    @classmethod
    def _load_test_cases(cls) -> List[Dict]:
        """Load test cases from CSV"""
        csv_file = "data/etl_validation_tests.csv"
        df = pd.read_csv(csv_file)
        df['enabled'] = df['enabled'].astype(str)
        enabled_df = df[df['enabled'].str.upper() == 'TRUE']
        return enabled_df.to_dict('records')
    
    @staticmethod
    def _execute_query_with_variables(client, query: str, variables: Dict[str, Any]) -> Any:
        """Execute query with variable substitution"""
        for var_name, var_value in variables.items():
            placeholder = f"{{{var_name}}}"
            if placeholder in query:
                if isinstance(var_value, list):
                    var_value = ','.join(map(str, var_value))
                query = query.replace(placeholder, str(var_value))
        return client.execute_query(query)
    
    @staticmethod
    def _extract_variables_from_query(query: str) -> List[str]:
        """Extract variable names from query"""
        return re.findall(r'\{(\w+)\}', query)
    
    def _execute_validation(self, test_case: Dict) -> Dict[str, Any]:
        """Execute validation based on test case configuration"""
        
        test_id = test_case['test_id']
        source_query = test_case['source_query']
        target_query = test_case['target_query']
        validation_type = test_case['validation_type']
        
        with allure.step(f"Execute source query for {test_id}"):
            source_variables = {}
            target_vars = self._extract_variables_from_query(target_query)
            
            source_results = self.source_client.execute_query(source_query)
            allure.attach(str(source_results[:10]), name='Source Query Results (sample)', 
                         attachment_type=allure.attachment_type.TEXT)
            
            if 'recid_list' in target_vars:
                recid_list = [row['recid'] for row in source_results]
                source_variables['recid_list'] = recid_list
                allure.attach(str(recid_list[:20]), name='RecID List (first 20)', 
                             attachment_type=allure.attachment_type.TEXT)
        
        with allure.step(f"Execute target query for {test_id}"):
            target_results = self._execute_query_with_variables(
                self.target_client, target_query, source_variables
            )
            allure.attach(str(target_results[:10]), name='Target Query Results (sample)', 
                         attachment_type=allure.attachment_type.TEXT)
        
        with allure.step(f"Execute validation: {validation_type}"):
            result = self._run_validation(
                validation_type, source_results, target_results, test_case
            )
            return result
    
    def _run_validation(self, validation_type: str, source_data: Any, 
                       target_data: Any, test_case: Dict) -> Dict[str, Any]:
        """Run specific validation type"""
        
        try:
            if validation_type == 'row_count_comparison':
                source_count, target_count = self.validator.row_count_comparison(
                    source_data, target_data
                )
                return {
                    'status': 'PASSED',
                    'validation_type': 'row_count_comparison',
                    'source_count': source_count,
                    'target_count': target_count,
                    'message': f'Row counts match: {source_count}'
                }
            
            elif validation_type == 'duplicate_checks_primary_keys':
                source_recids = set(row['recid'] for row in source_data)
                target_recids = set(row['recid'] for row in target_data)
                missing_recids = source_recids - target_recids
                
                if missing_recids:
                    return {
                        'status': 'FAILED',
                        'validation_type': 'duplicate_checks_primary_keys',
                        'source_count': len(source_recids),
                        'target_count': len(target_recids),
                        'missing_count': len(missing_recids),
                        'missing_recids': list(missing_recids)[:10],
                        'message': f'{len(missing_recids)} recids missing in target'
                    }
                
                return {
                    'status': 'PASSED',
                    'validation_type': 'duplicate_checks_primary_keys',
                    'source_count': len(source_recids),
                    'target_count': len(source_recids),
                    'matched_count': len(source_recids),
                    'message': f'All {len(source_recids)} recids found in target'
                }
            
            elif validation_type == 'aggregate_validations':
                source_value = list(source_data[0].values())[0] if source_data else 0
                target_value = list(target_data[0].values())[0] if target_data else 0
                
                if abs(source_value - target_value) < 0.01:
                    return {
                        'status': 'PASSED',
                        'validation_type': 'aggregate_validations',
                        'source_value': source_value,
                        'target_value': target_value,
                        'message': f'Aggregates match: {source_value}'
                    }
                else:
                    return {
                        'status': 'FAILED',
                        'validation_type': 'aggregate_validations',
                        'source_value': source_value,
                        'target_value': target_value,
                        'message': f'Aggregate mismatch: Source={source_value}, Target={target_value}'
                    }
            
            else:
                return {
                    'status': 'FAILED',
                    'validation_type': validation_type,
                    'message': f'Unknown validation type: {validation_type}'
                }
        
        except AssertionError as e:
            return {
                'status': 'FAILED',
                'validation_type': validation_type,
                'message': str(e)
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'validation_type': validation_type,
                'message': f'Validation error: {str(e)}'
            }
    
    def pytest_generate_tests(self, metafunc):
        """Generate tests dynamically from CSV"""
        if 'test_case' in metafunc.fixturenames:
            test_cases = self._load_test_cases()
            metafunc.parametrize('test_case', test_cases, ids=[tc['test_id'] for tc in test_cases])
    
    @pytest.mark.fabric
    @pytest.mark.etl
    def test_csv_driven_validation(self, test_case):
        """Execute CSV-driven ETL validation test"""
        
        test_id = test_case['test_id']
        test_name = test_case['test_name']
        description = test_case['description']
        severity = test_case.get('severity', 'normal')
        
        allure.dynamic.title(f"{test_id}: {test_name}")
        allure.dynamic.description(description)
        allure.dynamic.severity(severity)
        
        with allure.step("Test Configuration"):
            config_info = f"""Test ID: {test_id}
Test Name: {test_name}
Validation Type: {test_case['validation_type']}
Severity: {severity}"""
            allure.attach(config_info, name='Test Configuration', 
                         attachment_type=allure.attachment_type.TEXT)
        
        result = self._execute_validation(test_case)
        
        # Add custom parameters for graphs
        allure.dynamic.parameter("Validation Type", result.get('validation_type', 'N/A'))
        
        if 'source_count' in result:
            allure.dynamic.parameter("Source Count", result['source_count'])
        if 'target_count' in result:
            allure.dynamic.parameter("Target Count", result['target_count'])
        if 'source_value' in result:
            allure.dynamic.parameter("Source Value", result['source_value'])
        if 'target_value' in result:
            allure.dynamic.parameter("Target Value", result['target_value'])
        if 'matched_count' in result:
            allure.dynamic.parameter("Matched Records", result['matched_count'])
        
        with allure.step("Validation Results"):
            allure.attach(str(result), name='Validation Result', 
                         attachment_type=allure.attachment_type.JSON)
        
        assert result['status'] == 'PASSED', result.get('message', 'Validation failed')
        print(f"PASS: {test_id}: {result.get('message', 'PASSED')}")
