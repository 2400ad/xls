import xml.etree.ElementTree as ET
import re
from typing import Dict, List, Tuple, Optional

class QueryDifference:
    def __init__(self):
        self.is_equal = True
        self.differences = []
        self.query_type = None
        self.table_name = None
    
    def add_difference(self, column: str, value1: str, value2: str):
        self.is_equal = False
        self.differences.append({
            'column': column,
            'query1_value': value1,
            'query2_value': value2
        })

    def __str__(self) -> str:
        if self.is_equal:
            return "쿼리가 일치합니다."
        
        result = ["쿼리가 일치하지 않습니다:"]
        for diff in self.differences:
            result.append(f"  컬럼: {diff['column']}")
            result.append(f"    - Excel: {diff['query1_value']}")
            result.append(f"    - 파일: {diff['query2_value']}")
        return "\n".join(result)

class QueryParser:
    # 특수 컬럼 정의를 클래스 변수로 변경
    special_columns = {
        'send': {
            'required': ['EAI_SEQ_ID', 'DATA_INTERFACE_TYPE_CODE'],
            'mappings': []  # 추가 매핑을 저장할 리스트
        },
        'recv': {
            'required': [
                'EAI_SEQ_ID',
                'DATA_INTERFACE_TYPE_CODE',
                'EAI_INTERFACE_DATE',
                'APPLICATION_TRANSFER_FLAG'
            ],
            'special_values': {
                'EAI_INTERFACE_DATE': 'SYSDATE',
                'APPLICATION_TRANSFER_FLAG': "'N'"
            }
        }
    }

    def __init__(self):
        self.select_queries = []
        self.insert_queries = []

    def normalize_query(self, query):
        """
        Normalize a SQL query by removing extra whitespace and standardizing format
        
        Args:
            query (str): SQL query to normalize
            
        Returns:
            str: Normalized query
        """
        # Remove comments if any
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        
        # Replace multiple whitespace with single space
        query = re.sub(r'\s+', ' ', query)
        
        # Remove whitespace around common SQL punctuation
        query = re.sub(r'\s*(,|\(|\))\s*', r'\1', query)
        
        # Ensure single space after SQL keywords (case-insensitive match but preserve original case)
        for keyword in ['SELECT', 'FROM', 'WHERE', 'INTO', 'VALUES']:
            query = re.sub(f'(?i){keyword}\\s+', f'{keyword} ', query)
        
        return query.strip()

    def parse_select_columns(self, query) -> Optional[Dict[str, str]]:
        """Extract columns from SELECT query and return as dictionary"""
        # 정규화된 쿼리 사용
        query = self.normalize_query(query)
        
        # SELECT와 FROM 사이의 컬럼 추출
        match = re.search(r'SELECT\s+(.*?)\s+FROM', query, flags=re.IGNORECASE)
        if not match:
            return None
            
        columns = {}
        for col in match.group(1).split(','):
            col = col.strip()
            if col:  # 빈 문자열이 아닌 경우만 처리
                columns[col] = col
        return columns if columns else None

    def parse_insert_parts(self, query) -> Optional[Tuple[str, Dict[str, str]]]:
        """Extract and return table name and column-value pairs from INSERT query"""
        # 정규화된 쿼리 사용
        query = self.normalize_query(query)
        print(f"\nProcessing INSERT query:\n{query}")
        
        # INSERT INTO와 테이블 이름 추출
        table_match = re.search(r'INSERT\s+INTO\s+([A-Za-z0-9_$.]+)', query, flags=re.IGNORECASE)
        if not table_match:
            print("Failed to match INSERT INTO pattern")
            return None
            
        table_name = table_match.group(1)
        print(f"Found table name: {table_name}")
        
        # 컬럼과 값 추출
        pattern = r'\((.*?)\)\s*VALUES\s*\((.*?)\)'
        cols_match = re.search(pattern, query, flags=re.IGNORECASE | re.DOTALL)
        if not cols_match:
            print("Failed to match columns and values pattern")
            return None
            
        # 컬럼 파싱
        col_names = [c.strip() for c in cols_match.group(1).split(',')]
        
        # 값 파싱 - 함수 호출을 고려한 파싱
        values_str = cols_match.group(2)
        col_values = []
        current_value = ""
        paren_count = 0
        
        for char in values_str:
            if char == ',' and paren_count == 0:
                if current_value:
                    col_values.append(current_value.strip())
                    current_value = ""
            else:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                current_value += char
        
        if current_value:  # 마지막 값 추가
            col_values.append(current_value.strip())
        
        print(f"Found columns: {col_names}")
        print(f"Found values: {col_values}")
        
        # 컬럼과 값의 개수가 일치하는지 확인
        if len(col_names) != len(col_values):
            print(f"Column count ({len(col_names)}) does not match value count ({len(col_values)})")
            return None
            
        # 빈 컬럼이나 값이 있는지 확인
        if not all(col_names) or not all(col_values):
            print("Found empty column names or values")
            return None
            
        columns = {}
        for name, value in zip(col_names, col_values):
            columns[name] = value
            
        print(f"Successfully parsed {len(columns)} columns")
        return (table_name, columns) if columns else None

    def compare_queries(self, query1: str, query2: str) -> QueryDifference:
        """
        Compare two SQL queries and return detailed differences
        
        Args:
            query1 (str): First SQL query
            query2 (str): Second SQL query
            
        Returns:
            QueryDifference: Object containing comparison results and differences
        """
        result = QueryDifference()
        
        # 쿼리 정규화
        norm_query1 = self.normalize_query(query1)
        norm_query2 = self.normalize_query(query2)
        
        # 쿼리 타입 확인
        if re.search(r'SELECT', norm_query1, flags=re.IGNORECASE):
            result.query_type = 'SELECT'
            columns1 = self.parse_select_columns(query1)
            columns2 = self.parse_select_columns(query2)
            table1 = self.extract_table_name(query1)
            table2 = self.extract_table_name(query2)
            
            if columns1 is None or columns2 is None:
                raise ValueError("SELECT 쿼리 파싱 실패")
                
        elif re.search(r'INSERT', norm_query1, flags=re.IGNORECASE):
            result.query_type = 'INSERT'
            insert_result1 = self.parse_insert_parts(query1)
            insert_result2 = self.parse_insert_parts(query2)
            
            if insert_result1 is None or insert_result2 is None:
                raise ValueError("INSERT 쿼리 파싱 실패")
                
            table1, columns1 = insert_result1
            table2, columns2 = insert_result2
        else:
            raise ValueError("지원하지 않는 쿼리 타입입니다.")
            
        result.table_name = table1
        
        # 특수 컬럼 제외
        direction = 'recv' if result.query_type == 'INSERT' else 'send'
        special_cols = set(self.special_columns[direction]['required'])
        
        # 일반 컬럼만 비교 (대소문자 구분 없이 비교하되 원본 케이스 유지)
        columns1_filtered = {k: v for k, v in columns1.items() if k.upper() not in special_cols}
        columns2_filtered = {k: v for k, v in columns2.items() if k.upper() not in special_cols}
        
        # 컬럼 비교
        all_columns = set(columns1_filtered.keys()) | set(columns2_filtered.keys())
        for col in all_columns:
            if col not in columns1_filtered:
                result.add_difference(col, None, columns2_filtered[col])
            elif col not in columns2_filtered:
                result.add_difference(col, columns1_filtered[col], None)
            elif columns1_filtered[col] != columns2_filtered[col]:
                result.add_difference(col, columns1_filtered[col], columns2_filtered[col])
                
        return result

    def check_special_columns(self, query: str, direction: str) -> List[str]:
        """
        특수 컬럼의 존재 여부와 값을 체크합니다.
        
        Args:
            query (str): 검사할 쿼리
            direction (str): 송신('send') 또는 수신('recv')
            
        Returns:
            List[str]: 경고 메시지 리스트
        """
        warnings = []
        
        if direction == 'send':
            columns = self.parse_select_columns(query)
        else:
            _, columns = self.parse_insert_parts(query)
            
        if not columns:
            return warnings
            
        # 대소문자 구분 없이 컬럼 비교를 위한 매핑 make
        columns_upper = {k.upper(): (k, v) for k, v in columns.items()}
        
        # 필수 특수 컬럼 체크
        for col in self.special_columns[direction]['required']:
            if col not in columns_upper:
                warnings.append(f"필수 특수 컬럼 '{col}'이(가) {direction} 쿼리에 없습니다.")
        
        # 수신 쿼리의 특수 값 체크
        if direction == 'recv':
            for col, expected_value in self.special_columns[direction]['special_values'].items():
                if col in columns_upper:
                    actual_value = columns_upper[col][1].upper()
                    if actual_value != expected_value.upper():
                        warnings.append(f"특수 컬럼 '{col}'의 값이 '{expected_value}'이(가) 아닙니다. (현재 값: {columns_upper[col][1]})")
        
        return warnings

    def clean_select_query(self, query):
        """
        Clean SELECT query by removing WHERE clause
        """
        # Find the position of WHERE (case insensitive)
        where_match = re.search(r'\sWHERE\s', query, flags=re.IGNORECASE)
        if where_match:
            # Return only the part before WHERE
            return query[:where_match.start()].strip()
        return query.strip()

    def clean_insert_query(self, query: str) -> str:
        """
        Clean INSERT query by removing PL/SQL blocks
        """
        # PL/SQL 블록에서 INSERT 문 추출
        pattern = r"""
            (?:BEGIN\s+)?          # BEGIN (optional)
            (INSERT\s+INTO\s+      # INSERT INTO
            [^;]+                  # everything until semicolon
            )                      # capture this part
            (?:\s*;)?             # optional semicolon
            (?:\s*EXCEPTION\s+     # EXCEPTION block (optional)
            .*?                    # everything until END
            END;?)?                # END with optional semicolon
        """
        insert_match = re.search(
            pattern,
            query,
            flags=re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE
        )
        
        if insert_match:
            return insert_match.group(1).strip()
        return query.strip()

    def is_meaningful_query(self, query: str) -> bool:
        """
        Check if a query is meaningful (not just a simple existence check or count)
        
        Args:
            query (str): SQL query to analyze
            
        Returns:
            bool: True if the query is meaningful, False otherwise
        """
        query = query.lower()
        
        # Remove comments and normalize whitespace
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query = ' '.join(query.split())
        
        # Patterns for meaningless queries
        meaningless_patterns = [
            r'select\s+1\s+from',  # SELECT 1 FROM ...
            r'select\s+count\s*\(\s*\*\s*\)\s+from',  # SELECT COUNT(*) FROM ...
            r'select\s+count\s*\(\s*1\s*\)\s+from',  # SELECT COUNT(1) FROM ...
            r'select\s+null\s+from',  # SELECT NULL FROM ...
            r'select\s+\'[^\']*\'\s+from',  # SELECT 'constant' FROM ...
            r'select\s+\d+\s+from',  # SELECT {number} FROM ...
        ]
        
        # Check if query matches any meaningless pattern
        for pattern in meaningless_patterns:
            if re.search(pattern, query):
                return False
                
        # For SELECT queries, check if it's selecting actual columns
        if query.startswith('select'):
            # Extract the SELECT clause (between SELECT and FROM)
            select_match = re.match(r'select\s+(.+?)\s+from', query)
            if select_match:
                select_clause = select_match.group(1)
                # If only selecting literals or simple expressions, consider it meaningless
                if re.match(r'^[\d\'\"\s,]+$', select_clause):
                    return False
        
        return True

    def find_files_by_table(self, folder_path: str, table_name: str, skip_meaningless: bool = True) -> dict:
        """
        Find files containing queries that reference the specified table
        
        Args:
            folder_path (str): Path to the folder to search in
            table_name (str): Name of the DB table to search for
            skip_meaningless (bool): If True, skip queries that appear to be meaningless
            
        Returns:
            dict: Dictionary with 'select' and 'insert' as keys, each containing a list of tuples
                 where each tuple contains (file_path, query)
        """
        import os
        
        results = {
            'select': [],
            'insert': []
        }
        
        # Normalize table name for comparison
        table_name = table_name.lower()
        
        # Create parser instance for processing files
        parser = self
        
        # Walk through all files in the folder
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                # Skip non-XML files silently
                if not file_path.lower().endswith('.xml'):
                    continue
                    
                try:
                    # Try to parse queries from the file
                    select_queries, insert_queries = self.parse_xml_file(file_path)
                    
                    # Check SELECT queries
                    for query in select_queries:
                        if self.extract_table_name(query).lower() == table_name:
                            # Skip meaningless queries if requested
                            if skip_meaningless and not self.is_meaningful_query(query):
                                continue
                            rel_path = os.path.relpath(file_path, folder_path)
                            results['select'].append((rel_path, query))
                    
                    # Check INSERT queries
                    for query in insert_queries:
                        if self.extract_table_name(query).lower() == table_name:
                            rel_path = os.path.relpath(file_path, folder_path)
                            results['insert'].append((rel_path, query))
                    
                except Exception:
                    # Skip any errors silently
                    continue
        
        return results

    def parse_xml_file(self, filename):
        """
        Parse XML file and extract SQL queries
        
        Args:
            filename (str): Path to the XML file
            
        Returns:
            tuple: Lists of (select_queries, insert_queries)
        """
        try:
            # Clear previous queries
            self.select_queries = []
            self.insert_queries = []
            
            # Parse XML file
            tree = ET.parse(filename)
            root = tree.getroot()
            
            # Find all text content in the XML
            for elem in root.iter():
                if elem.text:
                    text = elem.text.strip()
                    # Extract SELECT queries
                    if re.search(r'SELECT\s+', text, flags=re.IGNORECASE):
                        cleaned_query = self.clean_select_query(text)
                        self.select_queries.append(cleaned_query)
                    # Extract INSERT queries
                    elif re.search(r'INSERT\s+INTO\s+', text, flags=re.IGNORECASE):
                        cleaned_query = self.clean_insert_query(text)
                        self.insert_queries.append(cleaned_query)
            
            return self.select_queries, self.insert_queries
            
        except ET.ParseError:
            return [], []
        except Exception:
            return [], []
    
    def get_select_queries(self):
        """Return list of extracted SELECT queries"""
        return self.select_queries
    
    def get_insert_queries(self):
        """Return list of extracted INSERT queries"""
        return self.insert_queries
    
    def print_queries(self):
        """Print all extracted queries"""
        print("\nSELECT Queries:")
        for i, query in enumerate(self.select_queries, 1):
            print(f"{i}. {query}\n")
            
        print("\nINSERT Queries:")
        for i, query in enumerate(self.insert_queries, 1):
            print(f"{i}. {query}\n")

    def print_query_differences(self, diff: QueryDifference):
        """Print the differences between two queries in a readable format"""
        print(f"\nQuery Type: {diff.query_type}")
        if diff.is_equal:
            print("Queries are equivalent")
        else:
            print("Differences found:")
            for d in diff.differences:
                print(f"- Column '{d['column']}':")
                print(f"  Query 1: {d['query1_value']}")
                print(f"  Query 2: {d['query2_value']}")

    def extract_table_name(self, query: str) -> str:
        """
        Extract table name from a SQL query
        
        Args:
            query (str): SQL query to analyze
            
        Returns:
            str: Table name or empty string if not found
        """
        query = self.normalize_query(query)
        
        # For SELECT queries
        select_match = re.search(r'from\s+([a-zA-Z0-9_$.]+)', query, flags=re.IGNORECASE)
        if select_match:
            return select_match.group(1)
            
        # For INSERT queries
        insert_match = re.search(r'insert\s+into\s+([a-zA-Z0-9_$.]+)', query, flags=re.IGNORECASE)
        if insert_match:
            return insert_match.group(1)
            
        return ""

    def print_table_search_results(self, results: dict, table_name: str):
        """
        Print table search results in a formatted way
        
        Args:
            results (dict): Dictionary with search results
            table_name (str): Name of the DB table that was searched
        """
        print(f"\nFiles and queries referencing table: {table_name}")
        print("=" * 50)
        
        print("\nSELECT queries found in:")
        if results['select']:
            for i, (file, query) in enumerate(results['select'], 1):
                print(f"\n{i}. File: {file}")
                print("Query:")
                print(query)
        else:
            print("  No files found with SELECT queries")
            
        print("\nINSERT queries found in:")
        if results['insert']:
            for i, (file, query) in enumerate(results['insert'], 1):
                print(f"\n{i}. File: {file}")
                print("Query:")
                print(query)
        else:
            print("  No files found with INSERT queries")
        
        print("\n" + "=" * 50)

class BWQueryExtractor:
    """TIBCO BW XML 파일에서 특정 태그 구조에 따라 SQL 쿼리를 추출하는 클래스"""
    
    def __init__(self):
        self.ns = {
            'pd': 'http://xmlns.tibco.com/bw/process/2003',
            'xsl': 'http://www.w3.org/1999/XSL/Transform',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
        }

    def _remove_oracle_hints(self, query: str) -> str:
        """
        SQL 쿼리에서 Oracle 힌트(/*+ ... */) 제거
        
        Args:
            query (str): 원본 SQL 쿼리
            
        Returns:
            str: 힌트가 제거된 SQL 쿼리
        """
        import re
        # /*+ ... */ 패턴의 힌트 제거
        cleaned_query = re.sub(r'/\*\+[^*]*\*/', '', query)
        # 불필요한 공백 정리 (여러 개의 공백을 하나로)
        cleaned_query = re.sub(r'\s+', ' ', cleaned_query).strip()
        
        if cleaned_query != query:
            print("\n=== Oracle 힌트 제거 ===")
            print(f"원본 쿼리: {query}")
            print(f"정리된 쿼리: {cleaned_query}")
            
        return cleaned_query

    def _get_parameter_names(self, activity) -> List[str]:
        """
        Prepared_Param_DataType에서 파라미터 이름 목록 추출
        
        Args:
            activity: JDBC 액티비티 XML 요소
            
        Returns:
            List[str]: 파라미터 이름 목록
        """
        param_names = []
        print("\n=== XML 구조 디버깅 ===")
        print("activity 태그:", activity.tag)
        print("activity의 자식 태그들:", [child.tag for child in activity])
        
        # 대소문자를 맞춰서 수정
        prepared_params = activity.find('.//Prepared_Param_DataType', self.ns)
        if prepared_params is not None:
            print("\n=== Prepared_Param_DataType 태그 발견 ===")
            print("prepared_params 태그:", prepared_params.tag)
            print("prepared_params의 자식 태그들:", [child.tag for child in prepared_params])
            
            for param in prepared_params.findall('./parameter', self.ns):
                param_name = param.find('./parameterName', self.ns)
                if param_name is not None and param_name.text:
                    name = param_name.text.strip()
                    param_names.append(name)
                    print(f"파라미터 이름 추출: {name}")
        else:
            print("\n=== Prepared_Param_DataType 태그를 찾을 수 없음 ===")
            # 전체 XML 구조를 재귀적으로 출력하여 디버깅
            def print_element_tree(element, level=0):
                print("  " * level + f"- {element.tag}")
                for child in element:
                    print_element_tree(child, level + 1)
            print("\n=== 전체 XML 구조 ===")
            print_element_tree(activity)
        
        return param_names

    def _replace_with_param_names(self, query: str, param_names: List[str]) -> str:
        """
        1단계: SQL 쿼리의 ? 플레이스홀더를 prepared_Param_DataType의 파라미터 이름으로 대체
        
        Args:
            query (str): 원본 SQL 쿼리
            param_names (List[str]): 파라미터 이름 목록
            
        Returns:
            str: 파라미터 이름이 대체된 SQL 쿼리
        """
        parts = query.split('?')
        if len(parts) == 1:  # 플레이스홀더가 없는 경우
            return query
            
        result = parts[0]
        for i, param_name in enumerate(param_names):
            if i < len(parts):
                result += f":{param_name}" + parts[i+1]
                
        print("\n=== 1단계: prepared_Param_DataType 매핑 결과 ===")
        print(f"원본 쿼리: {query}")
        print(f"매핑된 쿼리: {result}")
        return result

    def _get_record_mappings(self, activity, param_names: List[str]) -> Dict[str, str]:
        """
        2단계: Record 태그에서 실제 값 매핑 정보 추출
        
        Args:
            activity: JDBC 액티비티 XML 요소
            param_names: prepared_Param_DataType에서 추출한 파라미터 이름 목록
            
        Returns:
            Dict[str, str]: 파라미터 이름과 매핑된 실제 값의 딕셔너리
        """
        mappings = {}
        input_bindings = activity.find('.//pd:inputBindings', self.ns)
        if input_bindings is None:
            print("\n=== inputBindings 태그를 찾을 수 없음 ===")
            return mappings

        print("\n=== Record 매핑 검색 시작 ===")
        
        # jdbcUpdateActivityInput/Record 찾기
        jdbc_input = input_bindings.find('.//jdbcUpdateActivityInput')
        if jdbc_input is None:
            print("jdbcUpdateActivityInput을 찾을 수 없음")
            return mappings

        # for-each/Record 찾기
        for_each = jdbc_input.find('.//xsl:for-each', self.ns)
        record = for_each.find('./Record') if for_each is not None else jdbc_input
        
        if record is not None:
            print("Record 태그 발견")
            # 각 파라미터 이름에 대해 매핑 찾기
            for param_name in param_names:
                print(f"\n파라미터 '{param_name}' 매핑 검색:")
                param_element = record.find(f'.//{param_name}')
                if param_element is not None:
                    # value-of 체크
                    value_of = param_element.find('.//xsl:value-of', self.ns)
                    if value_of is not None:
                        select_attr = value_of.get('select', '')
                        if select_attr:
                            # select="BANANA"와 같은 형식에서 실제 값 추출
                            value = select_attr.split('/')[-1]
                            mappings[param_name] = value
                            print(f"value-of 매핑 발견: {param_name} -> {value}")
                    
                    # choose/when 체크
                    choose = param_element.find('.//xsl:choose', self.ns)
                    if choose is not None:
                        when = choose.find('.//xsl:when', self.ns)
                        if when is not None:
                            test_attr = when.get('test', '')
                            if 'exists(' in test_attr:
                                # exists(BANANA)와 같은 형식에서 변수 이름 추출
                                value = test_attr[test_attr.find('(')+1:test_attr.find(')')]
                                mappings[param_name] = value
                                print(f"choose/when 매핑 발견: {param_name} -> {value}")
                else:
                    print(f"'{param_name}'에 대한 매핑을 찾을 수 없음")

        return mappings

    def _replace_with_actual_values(self, query: str, mappings: Dict[str, str]) -> str:
        """
        2단계: 파라미터 이름을 Record에서 찾은 실제 값으로 대체
        
        Args:
            query (str): 1단계에서 파라미터 이름이 대체된 쿼리
            mappings (Dict[str, str]): 파라미터 이름과 실제 값의 매핑
            
        Returns:
            str: 실제 값이 대체된 SQL 쿼리
        """
        result = query
        for param_name, actual_value in mappings.items():
            result = result.replace(f":{param_name}", f":{actual_value}")
            
        print("\n=== 2단계: Record 매핑 결과 ===")
        print(f"1단계 쿼리: {query}")
        print(f"최종 쿼리: {result}")
        return result

    def extract_recv_query(self, xml_path: str) -> List[Tuple[str, str, str]]:
        """
        수신용 XML에서 SQL 쿼리와 파라미터가 매핑된 쿼리를 추출
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            List[Tuple[str, str, str]]: (원본 쿼리, 1단계 매핑 쿼리, 2단계 매핑 쿼리) 목록
        """
        queries = []
        try:
            print(f"\n=== XML 파일 처리 시작: {xml_path} ===")
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # JDBC 액티비티 찾기
            activities = root.findall('.//pd:activity', self.ns)
            
            for activity in activities:
                # JDBC 액티비티 타입 확인
                activity_type = activity.find('./pd:type', self.ns)
                if activity_type is None or 'jdbc' not in activity_type.text.lower():
                    continue
                    
                print(f"\nJDBC 액티비티 발견: {activity.get('name', 'Unknown')}")
                
                # statement 추출
                statement = activity.find('.//config/statement')
                if statement is not None and statement.text:
                    query = statement.text.strip()
                    print(f"\n발견된 쿼리:\n{query}")
                    
                    # SELECT 쿼리인 경우
                    if query.lower().startswith('select'):
                        # FROM DUAL 쿼리 제외
                        if not self._is_valid_query(query):
                            print("=> FROM DUAL 쿼리이므로 제외")
                            continue
                        # Oracle 힌트 제거
                        query = self._remove_oracle_hints(query)
                        print(f"=> Oracle 힌트 제거 후 쿼리:\n{query}")
                        queries.append((query, query, query))  # SELECT는 파라미터 매핑 없음
                    
                    # INSERT, UPDATE, DELETE 쿼리인 경우
                    elif query.lower().startswith(('insert', 'update', 'delete')):
                        # 1단계: prepared_Param_DataType의 파라미터 이름으로 매핑
                        param_names = self._get_parameter_names(activity)
                        stage1_query = self._replace_with_param_names(query, param_names)
                        
                        # 2단계: Record의 실제 값으로 매핑
                        mappings = self._get_record_mappings(activity, param_names)
                        stage2_query = self._replace_with_actual_values(stage1_query, mappings)
                        
                        queries.append((query, stage1_query, stage2_query))
                        print(f"=> 최종 처리된 쿼리:\n{stage2_query}")
            
            print(f"\n=== 처리된 유효한 쿼리 수: {len(queries)} ===")
            
        except ET.ParseError as e:
            print(f"\n=== XML 파싱 오류: {e} ===")
        except Exception as e:
            print(f"\n=== 쿼리 추출 중 오류 발생: {e} ===")
            
        return queries

    def _is_valid_query(self, query: str) -> bool:
        """
        분석 대상이 되는 유효한 쿼리인지 확인
        
        Args:
            query (str): SQL 쿼리
            
        Returns:
            bool: 유효한 쿼리이면 True
        """
        # 소문자로 변환하여 검사
        query_lower = query.lower()
        
        # SELECT FROM DUAL 패턴 체크
        if query_lower.startswith('select') and 'from dual' in query_lower:
            print(f"\n=== 단순 쿼리 제외 ===")
            print(f"제외된 쿼리: {query}")
            return False
            
        return True

    def extract_send_query(self, xml_path: str) -> List[str]:
        """
        송신용 XML에서 SQL 쿼리 추출
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            List[str]: SQL 쿼리 목록
        """
        queries = []
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # 송신 쿼리 추출 (Group 내의 SelectP 활동)
            select_activities = root.findall('.//pd:group[@name="Group"]//pd:activity[@name="SelectP"]', self.ns)
            
            print(f"\n=== 송신용 XML 처리 시작: {xml_path} ===")
            print(f"발견된 SelectP 활동 수: {len(select_activities)}")
            
            for activity in select_activities:
                statement = activity.find('.//config/statement')
                if statement is not None and statement.text:
                    query = statement.text.strip()
                    print(f"\n발견된 쿼리:\n{query}")
                    
                    # 1. 유효한 쿼리인지 먼저 확인
                    if not self._is_valid_query(query):
                        print("=> FROM DUAL 쿼리이므로 제외")
                        continue
                        
                    # 2. 유효한 쿼리에 대해서만 Oracle 힌트 제거
                    cleaned_query = self._remove_oracle_hints(query)
                    print(f"=> 최종 처리된 쿼리:\n{cleaned_query}")
                    queries.append(cleaned_query)
            
            print(f"\n=== 처리된 유효한 쿼리 수: {len(queries)} ===")
            
        except ET.ParseError as e:
            print(f"XML 파싱 오류: {e}")
        except Exception as e:
            print(f"쿼리 추출 중 오류 발생: {e}")
            
        return queries

    def extract_bw_queries(self, xml_path: str) -> Dict[str, List[str]]:
        """
        TIBCO BW XML 파일에서 송신/수신 쿼리를 모두 추출
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Dict[str, List[str]]: 송신/수신 쿼리 목록
                {
                    'send': [select 쿼리 목록],
                    'recv': [insert 쿼리 목록]
                }
        """
        return {
            'send': self.extract_send_query(xml_path),
            'recv': [mapped_query for _, _, mapped_query in self.extract_recv_query(xml_path)]
        }
    def get_single_query(self, xml_path: str) -> str:
        """
        BW XML 파일에서 SQL 쿼리를 추출하여 단일 문자열로 반환
        송신(send)과 수신(recv) 쿼리 중 존재하는 것을 반환
        둘 다 없는 경우 빈 문자열 반환
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            str: 추출된 SQL 쿼리 문자열. 쿼리가 없으면 빈 문자열
        """
        try:
            # 기존 extract_bw_queries 메소드 활용
            queries = self.extract_bw_queries(xml_path)
            
            # 송신 쿼리 확인
            if queries.get('send') and len(queries['send']) > 0:
                return queries['send'][0]  # 첫 번째 송신 쿼리 반환
                
            # 수신 쿼리 확인
            if queries.get('recv') and len(queries['recv']) > 0:
                return queries['recv'][0]  # 첫 번째 수신 쿼리 반환
                
            # 쿼리가 없는 경우
            return ""
            
        except Exception as e:
            print(f"쿼리 추출 중 오류 발생: {e}")
            return ""  # 오류 발생 시 빈 문자열 반환        

class FileSearcher:
    @staticmethod
    def find_files_with_keywords(folder_path: str, keywords: list) -> dict:
        """
        Search for files in the given folder that contain any of the specified keywords
        
        Args:
            folder_path (str): Path to the folder to search in
            keywords (list): List of keywords to search for
            
        Returns:
            dict: Dictionary with keyword as key and list of matching files as value
        """
        import os
        
        # Initialize results dictionary
        results = {keyword: [] for keyword in keywords}
        
        # Walk through all files in the folder
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                try:
                    # Try to read file content
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    # Check for each keyword
                    for keyword in keywords:
                        if keyword in content:
                            # Store relative path instead of full path
                            rel_path = os.path.relpath(file_path, folder_path)
                            results[keyword].append(rel_path)
                            
                except (UnicodeDecodeError, IOError):
                    # Skip files that can't be read as text
                    continue
        
        return results

    @staticmethod
    def print_search_results(results: dict):
        """
        Print search results in a formatted way
        
        Args:
            results (dict): Dictionary with keyword as key and list of matching files as value
        """
        print("\nSearch Results:")
        print("=" * 50)
        
        for keyword, files in results.items():
            print(f"\nKeyword: {keyword}")
            if files:
                print("Found in files:")
                for i, file in enumerate(files, 1):
                    print(f"  {i}. {file}")
            else:
                print("No files found containing this keyword")
        
        print("\n" + "=" * 50)

# Test the query comparison
if __name__ == "__main__":
    # Create QueryParser instance
    parser = QueryParser()
    
    # Test SELECT queries with different column orders
    select1 = """
    SELECT a, b, c FROM d
    """
    select2 = """
    SELECT b, a, c 
    FROM d
    """
    
    # Test INSERT queries with different values
    insert1 = """
    INSERT INTO table1 (a, b, c) VALUES ( :a, 'N', :code)
    """
    insert2 = """
    INSERT INTO table1 (a, c, b) 
    VALUES 
    ( :a, :code, 'Y')
    """
    
    # Compare queries and print differences
    select_diff = parser.compare_queries(select1, select2)
    insert_diff = parser.compare_queries(insert1, insert2)
    
    print("\nComparing SELECT queries:")
    print(f"Query 1:\n{select1}")
    print(f"Query 2:\n{select2}")
    parser.print_query_differences(select_diff)
    
    print("\nComparing INSERT queries:")
    print(f"Query 1:\n{insert1}")
    print(f"Query 2:\n{insert2}")
    parser.print_query_differences(insert_diff)

    # Test with the example queries from the previous test
    qry1 = """
    INSERT INTO A2EDC_MGR.TB_EDC_IFI_ECO_TARGET_GLS_N_I (
         EAI_SEQ_ID, DATA_INTERFACE_TYPE_CODE,
         EAI_INTERFACE_DATE, APPLICATION_TRANSFER_FLAG,
         ECO_ID, VALIDATION_SEQS,
         EMEMO_ID, LOT_ID,
         GLASS_ID, SLOT_ID,
         CASSETTE_ID, FMC_REQUEST_TITLE
            )
    VALUES (
         :EAI_SEQ_ID, :DATA_INTERFACE_TYPE_CODE,
         SYSDATE, 'N',
         :ECO_ID, :VALIDATION_SEQS,
         :EMEMO_ID, :LOT_ID,
         :GLASS_ID, :SLOT_ID,
         :CASSETTE_ID, :FMC_REQUEST_TITLE
    )
    """
    qry2 = """
    INSERT INTO A2EDC_MGR.TB_EDC_IFI_ECO_TARGET_GLS_N_I (
         EAI_SEQ_ID, 
         DATA_INTERFACE_TYPE_CODE,
         EAI_INTERFACE_DATE, APPLICATION_TRANSFER_FLAG,
         ECO_ID, VALIDATION_SEQS,
         EMEMO_ID, LOT_ID,
         GLASS_ID, SLOT_ID,
         CASSETTE_ID, FMC_REQUEST_TITLE
            )
    VALUES (
         :EAI_SEQ_ID, 
         :DATA_INTERFACE_TYPE_CODE,
         SYSDATE, 'Y',
         :ECO_ID, 
         :VALIDATION_SEQS,
         :EMEMO_ID, :LOT_ID,
         :GLASS_ID, :SLOT_ID,
         :CASSETTE_ID, :FMC_REQUEST_TITLE
    )
    """
    
    complex_diff = parser.compare_queries(qry1, qry2)
    print("\nComparing complex INSERT queries:")
    print(f"Query 1:\n{qry1}")
    print(f"Query 2:\n{qry2}")
    parser.print_query_differences(complex_diff)

    # Test with XML file if provided
    try:
        parser = QueryParser()
        select_queries, insert_queries = parser.parse_xml_file("mq_snd.xml")
        parser.print_queries()
    except:
        pass

    parser = QueryParser()
    select_queries, insert_queries = parser.parse_xml_file("mq_rcv.xml")
    parser.print_queries()  # To see all extracted queries

    # Test file search functionality
    print("\nTesting file search functionality:")
    folder_path = "."  # Current directory
    keywords = ["GSMOD.H8ASSY", "RCV"]
    
    searcher = FileSearcher()
    results = searcher.find_files_with_keywords(folder_path, keywords)
    searcher.print_search_results(results)

    # Test table search functionality
    print("\nTesting table search functionality:")
    folder_path = "."  # Current directory
    table_name = "XXWMSV.XXWMSV_KIT_RCPT_S_I"  # Example table name
    
    parser = QueryParser()
    table_results = parser.find_files_by_table(folder_path, table_name)
    parser.print_table_search_results(table_results, table_name)
