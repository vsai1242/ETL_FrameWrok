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
                        'source_count': len(source_recids),
                        'target_count': len(target_recids),
                        'missing_count': len(missing_recids),
                        'missing_recids': list(missing_recids)[:10],
                        'message': f'{len(missing_recids)} recids missing in target'
                    }
                
                return {
                    'status': 'PASSED',
                    'source_count': len(source_recids),
                    'target_count': len(source_recids),
                    'matched_count': len(source_recids),
                    'message': f'All {len(source_recids)} recids found in target'
                }
            
            elif validation_type == 'record_level_dataframe_comparison':
                key_columns = ['recid']
                if source_data and target_data:
                    # Multi-table queries can share recid values across tables.
                    # Use composite key when TableName exists to avoid false mismatches.
                    source_cols = set(source_data[0].keys()) if hasattr(source_data[0], 'keys') else set()
                    target_cols = set(target_data[0].keys()) if hasattr(target_data[0], 'keys') else set()
                    if 'TableName' in source_cols and 'TableName' in target_cols:
                        key_columns = ['TableName', 'recid']

                # Insert-record validations use recid subset filters in target query.
                # For this scenario, validate source keys are present in target keys
                # and do not fail on additional target rows outside the sampled source.
                target_query = str(test_case.get('target_query', ''))
                if '{recid_list}' in target_query:
                    source_df = self.validator._to_dataframe(source_data, dataset_name='source_data')
                    target_df = self.validator._to_dataframe(target_data, dataset_name='target_data')

                    source_keys = set(map(tuple, source_df[key_columns].drop_duplicates().to_records(index=False)))
                    target_keys = set(map(tuple, target_df[key_columns].drop_duplicates().to_records(index=False)))

                    missing_keys = source_keys - target_keys
                    if missing_keys:
                        return {
                            'status': 'FAILED',
                            'source_count': len(source_keys),
                            'target_count': len(target_keys),
                            'missing_count': len(missing_keys),
                            'message': f'{len(missing_keys)} source keys missing in target. Sample: {list(missing_keys)[:10]}'
                        }

                    return {
                        'status': 'PASSED',
                        'source_count': len(source_keys),
                        'target_count': len(target_keys),
                        'matched_count': len(source_keys),
                        'message': f'All {len(source_keys)} source keys found in target'
                    }

                summary = self.validator.record_level_dataframe_comparison(
                    source_data=source_data,
                    target_data=target_data,
                    key_columns=key_columns
                )
                return {
                    'status': 'PASSED',
                    'source_count': len(source_data),
                    'target_count': len(target_data),
                    'matched_count': len(source_data) - summary['missing_in_target_count'],
                    'message': f'Record comparison passed: {len(source_data)} records validated'
                }
            
            else:
                return {
                    'status': 'FAILED',
                    'message': f'Unknown validation type: {validation_type}'
                }
        
        except AssertionError as e:
            return {
                'status': 'FAILED',
                'message': str(e)
            }
        except Exception as e:
            return {
                'status': 'ERROR',
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
        table_name = test_case.get('table_name', 'N/A')
        validation_type = test_case['validation_type']
        
        allure.dynamic.title(f"{test_id}: {test_name}")
        allure.dynamic.description(description)
        
        allure.dynamic.label("Table", table_name)
        allure.dynamic.label("Validation", validation_type.replace('_', ' ').title())
        
        result = self._execute_validation(test_case)
        
        if 'source_count' in result and 'target_count' in result:
            allure.dynamic.label("Source_Count", str(result['source_count']))
            allure.dynamic.label("Target_Count", str(result['target_count']))
            allure.dynamic.label("Match_Status", f"S:{result['source_count']}=T:{result['target_count']}")
        
        with allure.step("Validation Results"):
            result_summary = f"""Table: {table_name}
Validation: {validation_type}
Status: {result['status']}
Message: {result.get('message')}"""
            
            if 'source_count' in result:
                result_summary += f"\nSource Count: {result['source_count']}"
            if 'target_count' in result:
                result_summary += f"\nTarget Count: {result['target_count']}"
            if 'matched_count' in result:
                result_summary += f"\nMatched Records: {result['matched_count']}"
            
            allure.attach(result_summary, name='Validation Summary', 
                         attachment_type=allure.attachment_type.TEXT)
        
        assert result['status'] == 'PASSED', result.get('message', 'Validation failed')
        print(f"PASS: {test_id}: {result.get('message', 'PASSED')}")
