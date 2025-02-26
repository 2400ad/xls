import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from xltest import process_interface, read_interface_block
from comp_q import QueryParser, QueryDifference, FileSearcher, BWQueryExtractor
from maptest import ColumnMapper
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
import os
import fnmatch
import sys
import datetime
import ast

def read_interface_block(ws, start_col):
    """Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다.
    이 함수는 xltest.py의 동일한 함수를 대체하지 않고, 가져오지 못한 경우의 백업 역할만 합니다.
    """
    try:
        interface_info = {
            'interface_name': ws.cell(row=1, column=start_col).value or '',  # IF NAME (1행)
            'interface_id': ws.cell(row=2, column=start_col).value or '',    # IF ID (2행)
            'send': {'owner': None, 'table_name': None, 'columns': [], 'db_info': None},
            'recv': {'owner': None, 'table_name': None, 'columns': [], 'db_info': None}
        }
        
        # 인터페이스 ID가 없으면 빈 인터페이스로 간주
        if not interface_info['interface_id']:
            return None
            
        # DB 연결 정보 (3행에서 읽기)
        send_db_info = ast.literal_eval(ws.cell(row=3, column=start_col).value)
        recv_db_info = ast.literal_eval(ws.cell(row=3, column=start_col + 1).value)
        interface_info['send']['db_info'] = send_db_info
        interface_info['recv']['db_info'] = recv_db_info
        
        # 테이블 정보 (4행에서 읽기)
        send_table_info = ast.literal_eval(ws.cell(row=4, column=start_col).value)
        recv_table_info = ast.literal_eval(ws.cell(row=4, column=start_col + 1).value)
        
        interface_info['send']['owner'] = send_table_info.get('owner')
        interface_info['send']['table_name'] = send_table_info.get('table_name')
        interface_info['recv']['owner'] = recv_table_info.get('owner')
        interface_info['recv']['table_name'] = recv_table_info.get('table_name')
        
        # 컬럼 매핑 정보 (5행부터)
        row = 5
        while True:
            send_col = ws.cell(row=row, column=start_col).value
            recv_col = ws.cell(row=row, column=start_col + 1).value
            
            if not send_col and not recv_col:
                break
                
            interface_info['send']['columns'].append(send_col if send_col else '')
            interface_info['recv']['columns'].append(recv_col if recv_col else '')
            row += 1
            
    except Exception as e:
        print(f'인터페이스 정보 읽기 중 오류 발생: {str(e)}')
        return None
    
    return interface_info

class XMLComparator:
    # 클래스 변수로 BW_SEARCH_DIR 정의
    BW_SEARCH_DIR = "C:\\work\\LT\\BW소스"

    def __init__(self, excel_path: str, search_dir: str):
        """
        XML 비교를 위한 클래스 초기화
        
        Args:
            excel_path (str): 인터페이스 정보가 있는 Excel 파일 경로
            search_dir (str): XML 파일을 검색할 디렉토리 경로
        """
        self.excel_path = excel_path
        self.search_dir = search_dir
        self.workbook = openpyxl.load_workbook(excel_path)
        self.worksheet = self.workbook.active
        self.mapper = ColumnMapper()
        self.query_parser = QueryParser()
        self.output_wb = None  # 결과를 저장할 엑셀 워크북
        self.interface_results = []  # 모든 인터페이스 처리 결과 저장
        self.output_path = 'C:\\work\\LT\\comp_mq_bw.xlsx'  # 기본 출력 경로

    def extract_from_xml(self, xml_path: str) -> Tuple[str, str]:
        """
        XML 파일에서 쿼리와 XML 내용을 추출합니다.
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Tuple[str, str]: (쿼리, XML 내용)
        """
        try:
            # XML 파일이 제대로 로드되었는지 확인
            if not os.path.exists(xml_path):
                print(f"Warning: XML file not found: {xml_path}")
                return None, None
                
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # XML 내용이 유효한지 확인
            if root is None:
                print(f"Warning: Invalid XML content in file: {xml_path}")
                return None, None
            
            # SQL 노드 찾기
            sql_node = root.find(".//SQL")
            if sql_node is None or not sql_node.text:
                print(f"Warning: No SQL content found in file: {xml_path}")
                return None, None
                
            query = sql_node.text.strip()
            xml_content = ET.tostring(root, encoding='unicode')
            
            # 추출된 쿼리가 유효한지 확인
            if not query:
                print(f"Warning: Empty SQL query in file: {xml_path}")
                return None, None
                
            return query, xml_content
            
        except ET.ParseError as e:
            print(f"Error parsing XML file {xml_path}: {e}")
            return None, None
        except Exception as e:
            print(f"Unexpected error processing file {xml_path}: {e}")
            return None, None
            
    def compare_queries(self, query1: str, query2: str) -> QueryDifference:
        """
        두 쿼리를 비교합니다.
        
        Args:
            query1 (str): 첫 번째 쿼리
            query2 (str): 두 번째 쿼리
            
        Returns:
            QueryDifference: 쿼리 비교 결과
        """
        if not query1 or not query2:
            return None
        return self.query_parser.compare_queries(query1, query2)
        
    def find_interface_files(self, if_id: str) -> Dict[str, Dict]:
        """
        주어진 IF ID에 해당하는 송수신 XML 파일을 찾고 쿼리를 추출합니다.
        파일명 패턴: {if_id}로 시작하고 .SND.xml 또는 .RCV.xml로 끝나는 파일
        
        Args:
            if_id (str): 인터페이스 ID
            
        Returns:
            Dict[str, Dict]: {
                'send': {'path': 송신파일경로, 'query': 송신쿼리, 'xml': 송신XML},
                'recv': {'path': 수신파일경로, 'query': 수신쿼리, 'xml': 수신XML}
            }
        """
        results = {
            'send': {'path': None, 'query': None, 'xml': None},
            'recv': {'path': None, 'query': None, 'xml': None}
        }
        
        if not if_id:
            print("Warning: Empty IF_ID provided")
            return results
            
        try:
            # 디렉토리 내의 모든 XML 파일 검색
            for file in os.listdir(self.search_dir):
                if not file.startswith(if_id):
                    continue
                    
                file_path = os.path.join(self.search_dir, file)
                
                # 송신 파일 (.SND.xml)
                if file.endswith('.SND.xml'):
                    results['send']['path'] = file_path
                    query, xml = self.extract_from_xml(file_path)
                    if query and xml:
                        results['send']['query'] = query
                        results['send']['xml'] = xml
                    else:
                        print(f"Warning: Failed to extract query from send file: {file_path}")
                
                # 수신 파일 (.RCV.xml)
                elif file.endswith('.RCV.xml'):
                    results['recv']['path'] = file_path
                    query, xml = self.extract_from_xml(file_path)
                    if query and xml:
                        results['recv']['query'] = query
                        results['recv']['xml'] = xml
                    else:
                        print(f"Warning: Failed to extract query from receive file: {file_path}")
            
            # 파일을 찾았는지 확인
            if not results['send']['path'] and not results['recv']['path']:
                print(f"Warning: No interface files found for IF_ID: {if_id}")
            elif not results['send']['path']:
                print(f"Warning: No send file found for IF_ID: {if_id}")
            elif not results['recv']['path']:
                print(f"Warning: No receive file found for IF_ID: {if_id}")
            
            return results
            
        except Exception as e:
            print(f"Error finding interface files: {e}")
            return results
        
    def process_interface_block(self, start_col: int) -> Optional[Dict]:
        """
        Excel에서 하나의 인터페이스 블록을 처리합니다.
        
        Args:
            start_col (int): 인터페이스 블록이 시작되는 컬럼
            
        Returns:
            Optional[Dict]: 처리된 인터페이스 정보와 결과, 실패시 None
        """
        try:
            # Excel에서 인터페이스 정보 읽기
            interface_info = read_interface_block(self.worksheet, start_col)
            if not interface_info:
                print(f"Warning: Failed to read interface block at column {start_col}")
                return None
                
            # Excel에서 추출된 쿼리와 XML 얻기
            excel_results = process_interface(interface_info, self.mapper)
            if not excel_results:
                print(f"Warning: Failed to process interface at column {start_col}")
                return None
                
            # 송수신 파일 찾기
            file_results = self.find_interface_files(interface_info['interface_id'])
            if not file_results:
                print(f"Warning: No interface files found for IF_ID: {interface_info['interface_id']}")
                return None
            
            # 결과 초기화
            comparisons = {
                'send': None,
                'recv': None
            }
            warnings = {
                'send': [],
                'recv': []
            }
            
            # 송신 쿼리 처리
            if excel_results['send_sql'] and file_results['send']['query']:
                try:
                    comparisons['send'] = self.query_parser.compare_queries(
                        excel_results['send_sql'],
                        file_results['send']['query']
                    )
                    warnings['send'].extend(
                        self.query_parser.check_special_columns(
                            file_results['send']['query'],
                            'send'
                        )
                    )
                except Exception as e:
                    print(f"Error comparing send queries: {e}")
                    print(f"Excel query: {excel_results['send_sql']}")
                    print(f"File query: {file_results['send']['query']}")
            
            # 수신 쿼리 처리
            if excel_results['recv_sql'] and file_results['recv']['query']:
                try:
                    comparisons['recv'] = self.query_parser.compare_queries(
                        excel_results['recv_sql'],
                        file_results['recv']['query']
                    )
                    warnings['recv'].extend(
                        self.query_parser.check_special_columns(
                            file_results['recv']['query'],
                            'recv'
                        )
                    )
                except Exception as e:
                    print(f"Error comparing receive queries: {e}")
                    print(f"Excel query: {excel_results['recv_sql']}")
                    print(f"File query: {file_results['recv']['query']}")
            
            return {
                'if_id': interface_info['interface_id'],
                'interface_name': interface_info['interface_name'],
                'comparisons': comparisons,
                'warnings': warnings,
                'excel': excel_results,
                'files': file_results
            }
            
        except Exception as e:
            print(f"Error processing interface block at column {start_col}: {e}")
            return None
            
    def process_all_interfaces(self) -> List[Dict]:
        """
        Excel 파일의 모든 인터페이스를 처리합니다.
        B열부터 시작하여 3컬럼 단위로 처리합니다.
        
        Returns:
            List[Dict]: 각 인터페이스의 처리 결과 목록
        """
        results = []
        col = 2  # B열부터 시작
        
        while True:
            # 인터페이스 ID가 없으면 종료
            if not self.worksheet.cell(row=2, column=col).value:
                break
                
            result = self.process_interface_block(col)
            if result:
                results.append(result)
                
            col += 3  # 다음 인터페이스 블록으로 이동
            
        # 결과 출력
        for idx, result in enumerate(results, 1):
            print(f"\n=== 인터페이스 {idx} ===")
            print(f"ID: {result['if_id']}")
            print(f"이름: {result['interface_name']}")
            
            print("\n파일 검색 결과:")
            print(f"송신 파일: {result['files']['send']['path']}")
            print(f"수신 파일: {result['files']['recv']['path']}")
            
            print("\n쿼리 비교 결과:")
            if result['comparisons']['send']:
                print("송신 쿼리:")
                print(f"  {result['comparisons']['send']}")
            if result['comparisons']['recv']:
                print("수신 쿼리:")
                print(f"  {result['comparisons']['recv']}")
            
            # 경고가 있을 때만 경고 섹션 출력
            send_warnings = result['warnings']['send']
            recv_warnings = result['warnings']['recv']
            if send_warnings or recv_warnings:
                print("\n경고:")
                if send_warnings:
                    print("송신 쿼리 경고:")
                    for warning in send_warnings:
                        print(f"  - {warning}")
                if recv_warnings:
                    print("수신 쿼리 경고:")
                    for warning in recv_warnings:
                        print(f"  - {warning}")
                
        return results
        
    def close(self):
        """리소스 정리"""
        self.workbook.close()
        if self.mapper:
            self.mapper.close_connections()

    def find_bw_files(self) -> List[Dict[str, str]]:
        """
        엑셀의 인터페이스 정보에서 송신 테이블명을 추출하여 BW 파일을 검색합니다.
        
        Returns:
            List[Dict[str, str]]: [
                {
                    'interface_name': str,
                    'interface_id': str,
                    'send_table': str,
                    'bw_files': List[str]
                },
                ...
            ]
        """
        results = []
        file_searcher = FileSearcher()
        
        # 엑셀에서 인터페이스 정보 읽기
        for row in range(2, self.worksheet.max_row + 1, 3):  # 3행씩 건너뛰며 읽기
            interface_info = read_interface_block(self.worksheet, row)
            if not interface_info:
                continue
                
            # 송신 테이블명 추출 (스키마/오너 제외)
            send_table = interface_info['send'].get('table_name')
            if not send_table:
                continue
                
            # BW 파일 검색 - self.BW_SEARCH_DIR 사용
            bw_files = file_searcher.find_files_with_keywords(self.BW_SEARCH_DIR, [send_table])
            matching_files = bw_files.get(send_table, [])
            
            results.append({
                'interface_name': interface_info['interface_name'],
                'interface_id': interface_info['interface_id'],
                'send_table': send_table,
                'bw_files': matching_files
            })
            
        return results
        
    def print_bw_search_results(self, results: List[Dict[str, str]]):
        """
        BW 파일 검색 결과를 출력합니다.
        
        Args:
            results (List[Dict[str, str]]): find_bw_files()의 반환값
        """
        print("\nBW File Search Results:")
        print("-" * 80)
        print(f"{'Interface Name':<30} {'Interface ID':<15} {'Send Table':<20} {'BW Files'}")
        print("-" * 80)
        
        for result in results:
            bw_files_str = ', '.join(result['bw_files']) if result['bw_files'] else 'No matching files'
            print(f"{result['interface_name']:<30} {result['interface_id']:<15} {result['send_table']:<20} {bw_files_str}")

    def initialize_excel_output(self):
        """
        결과를 저장할 새 엑셀 파일 초기화
        """
        self.output_wb = openpyxl.Workbook()
        # 기본 시트의 이름을 변경
        if 'Sheet' in self.output_wb.sheetnames:
            sheet = self.output_wb['Sheet']
            sheet.title = '요약'
    
    def save_excel_output(self, output_path='C:\\work\\LT\\comp_mq_bw.xlsx'):
        """
        처리된 결과를 엑셀 파일로 저장
        
        Args:
            output_path (str): 출력 엑셀 파일 경로
        """
        # output_path 값을 인스턴스 변수에 저장
        self.output_path = output_path
        
        if not self.output_wb:
            print("결과를 저장할 워크북이 초기화되지 않았습니다.")
            return False
        
        try:
            self.output_wb.save(output_path)
            print(f"\n[결과 저장 완료] 파일 경로: {output_path}")
            return True
        except Exception as e:
            print(f"엑셀 파일 저장 중 오류 발생: {e}")
            return False
    
    def create_interface_sheet(self, index, interface_info, file_results, query_comparisons, bw_queries=None):
        """
        엑셀 파일에 각 인터페이스별 시트를 생성하고, 인터페이스 정보 및 쿼리 비교 결과를 기록
        
        Args:
            index (int): 인터페이스 순번
            interface_info (dict): 인터페이스 정보
            file_results (dict): 파일 검색 결과
            query_comparisons (dict): 쿼리 비교 결과
            bw_queries (dict, optional): BW 쿼리 정보
        """
        if not bw_queries:
            bw_queries = {'send': '', 'recv': ''}
        
        # 시트 이름 생성 (인터페이스 이름)
        sheet_name = interface_info['interface_name']
        if not sheet_name:
            sheet_name = f"인터페이스_{index}"
            
        # 이름이 너무 길면 잘라냄 (Excel 시트명 최대 길이는 31자)
        if len(sheet_name) > 31:
            sheet_name = sheet_name[:31]
        
        # 시트 생성
        sheet = self.output_wb.create_sheet(title=sheet_name)
        
        # 스타일 정의
        header_fill = PatternFill(start_color="CCCCFF", end_color="CCCCFF", fill_type="solid")
        subheader_fill = PatternFill(start_color="EEEEEE", end_color="EEEEEE", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        align_left = Alignment(horizontal='left', vertical='center', wrap_text=True)
        
        # 1. 인터페이스 정보 헤더
        row = 1
        sheet.merge_cells(f'A{row}:F{row}')
        cell = sheet.cell(row=row, column=1, value="인터페이스 정보")
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = align_center
        cell.border = border
        
        # 2. 인터페이스 정보 헤더 상세
        row += 1
        headers = ["인터페이스 ID", "인터페이스 명", "송신 시스템", "수신 시스템", "송신 테이블", "수신 테이블"]
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=row, column=col, value=header)
            cell.font = Font(bold=True)
            cell.fill = subheader_fill
            cell.alignment = align_center
            cell.border = border
            sheet.column_dimensions[get_column_letter(col)].width = 20
        
        # 3. 인터페이스 정보 값
        row += 1
        
        # 인터페이스 정보 가져오기
        if_id = interface_info.get('interface_id', '')
        if_name = interface_info.get('interface_name', '')
        
        send_system = interface_info.get('send_system', 'N/A')
        recv_system = interface_info.get('recv_system', 'N/A')
        
        send_table = interface_info.get('send_table', '')
        recv_table = interface_info.get('recv_table', '')
        
        # DB SID 정보 추출
        send_sid = interface_info.get('send', {}).get('db_info', {}).get('sid', 'N/A')
        recv_sid = interface_info.get('recv', {}).get('db_info', {}).get('sid', 'N/A')
        
        values = [if_id, if_name, f"{send_system} ({send_sid})", f"{recv_system} ({recv_sid})", send_table, recv_table]
        for col, value in enumerate(values, 1):
            cell = sheet.cell(row=row, column=col, value=value)
            cell.alignment = align_left
            cell.border = border
        
        # 4. 송신 쿼리 비교 섹션
        row += 2
        sheet.merge_cells(f'A{row}:E{row}')
        cell = sheet.cell(row=row, column=1, value="송신 SELECT 쿼리 비교")
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = align_center
        cell.border = border
        
        # 5. 송신 쿼리 비교 헤더
        row += 1
        headers = ["MQ 송신 쿼리", "BW 송신 쿼리", "비교 결과"]
        sheet.column_dimensions['A'].width = 40
        sheet.column_dimensions['B'].width = 40
        sheet.column_dimensions['C'].width = 30
        
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=row, column=col, value=header)
            cell.font = Font(bold=True)
            cell.fill = subheader_fill
            cell.alignment = align_center
            cell.border = border
        
        # 6. 송신 쿼리 값
        row += 1
        mq_send_query = file_results['send']['query'] if file_results['send']['query'] else '쿼리 없음'
        bw_send_query = bw_queries['send'] if bw_queries['send'] else '쿼리 없음'
        
        # MQ 송신 쿼리
        cell = sheet.cell(row=row, column=1, value=mq_send_query)
        cell.alignment = Alignment(wrap_text=True, vertical='top')
        cell.border = border
        sheet.row_dimensions[row].height = 120
        
        # BW 송신 쿼리
        cell = sheet.cell(row=row, column=2, value=bw_send_query)
        cell.alignment = Alignment(wrap_text=True, vertical='top')
        cell.border = border
        
        # 송신 쿼리 비교 결과
        diff_text = ["비교 결과 없음"]
        if query_comparisons and 'send' in query_comparisons and query_comparisons['send']:
            diff = query_comparisons['send']
            diff_text = []
            
            if not diff.is_equal:
                diff_text.append("※ 차이점 있음")
                for difference in diff.differences:
                    diff_text.append(f"- 컬럼: {difference['column']}")
                    diff_text.append(f"  - MQ: {difference['query1_value']}")
                    diff_text.append(f"  - BW: {difference['query2_value']}")
            else:
                diff_text.append("※ 차이점 없음")
        
        cell = sheet.cell(row=row, column=3, value="\n".join(diff_text))
        cell.alignment = Alignment(wrap_text=True, vertical='top')
        cell.border = border
        
        # 7. 수신 쿼리 비교 섹션
        row += 2
        sheet.merge_cells(f'A{row}:E{row}')
        cell = sheet.cell(row=row, column=1, value="수신 INSERT 쿼리 비교")
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = align_center
        cell.border = border
        
        # 8. 수신 쿼리 비교 헤더
        row += 1
        headers = ["MQ 수신 쿼리", "BW 수신 쿼리", "비교 결과"]
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=row, column=col, value=header)
            cell.font = Font(bold=True)
            cell.fill = subheader_fill
            cell.alignment = align_center
            cell.border = border
        
        # 9. 수신 쿼리 값
        row += 1
        mq_recv_query = file_results['recv']['query'] if file_results['recv']['query'] else '쿼리 없음'
        bw_recv_query = bw_queries['recv'] if bw_queries['recv'] else '쿼리 없음'
        
        # MQ 수신 쿼리
        cell = sheet.cell(row=row, column=1, value=mq_recv_query)
        cell.alignment = Alignment(wrap_text=True, vertical='top')
        cell.border = border
        sheet.row_dimensions[row].height = 120
        
        # BW 수신 쿼리
        cell = sheet.cell(row=row, column=2, value=bw_recv_query)
        cell.alignment = Alignment(wrap_text=True, vertical='top')
        cell.border = border
        
        # 수신 쿼리 비교 결과
        diff_text = ["비교 결과 없음"]
        if query_comparisons and 'recv' in query_comparisons and query_comparisons['recv']:
            diff = query_comparisons['recv']
            diff_text = []
            
            if not diff.is_equal:
                diff_text.append("※ 차이점 있음")
                for difference in diff.differences:
                    diff_text.append(f"- 컬럼: {difference['column']}")
                    diff_text.append(f"  - MQ: {difference['query1_value']}")
                    diff_text.append(f"  - BW: {difference['query2_value']}")
            else:
                diff_text.append("※ 차이점 없음")
        
        cell = sheet.cell(row=row, column=3, value="\n".join(diff_text))
        cell.alignment = Alignment(wrap_text=True, vertical='top')
        cell.border = border

    def process_interface_with_bw(self, start_col: int) -> Optional[Dict]:
        """
        하나의 인터페이스를 처리하고 BW 파일과 비교하여 결과 반환
        
        Args:
            start_col (int): 인터페이스 블록이 시작되는 컬럼
            
        Returns:
            Optional[Dict]: 처리된 인터페이스 정보와 결과, 실패시 None
        """
        try:
            # Excel에서 인터페이스 정보 읽기
            interface_info = read_interface_block(self.worksheet, start_col)
            if not interface_info:
                print(f"Warning: Failed to read interface block at column {start_col}")
                return None
                
            # 표준 필드 생성
            # DB 정보에서 시스템 정보 추출
            if 'send' in interface_info and 'db_info' in interface_info['send'] and interface_info['send']['db_info']:
                interface_info['send_system'] = interface_info['send']['db_info'].get('system', 'N/A')
            else:
                interface_info['send_system'] = 'N/A'
                
            if 'recv' in interface_info and 'db_info' in interface_info['recv'] and interface_info['recv']['db_info']:
                interface_info['recv_system'] = interface_info['recv']['db_info'].get('system', 'N/A')
            else:
                interface_info['recv_system'] = 'N/A'
                
            # 테이블 정보 추출
            if 'send' in interface_info and 'table_name' in interface_info['send']:
                interface_info['send_table'] = interface_info['send']['table_name']
            else:
                interface_info['send_table'] = ''
                
            if 'recv' in interface_info and 'table_name' in interface_info['recv']:
                interface_info['recv_table'] = interface_info['recv']['table_name']
            else:
                interface_info['recv_table'] = ''
                
            # Excel에서 추출된 쿼리와 XML 얻기
            excel_results = process_interface(interface_info, self.mapper)
            if not excel_results:
                print(f"Warning: Failed to process interface at column {start_col}")
                return None
                
            # 송수신 파일 찾기
            file_results = self.find_interface_files(interface_info['interface_id'])
            if not file_results:
                print(f"Warning: No interface files found for IF_ID: {interface_info['interface_id']}")
                return None
            
            # BW 파일 찾기
            send_table = interface_info.get('send_table', '')
            if not send_table:
                print(f"Warning: No send table information for IF_ID: {interface_info['interface_id']}")
                bw_files = []
            else:
                # 송신 테이블로 BW 파일 검색
                bw_searcher = FileSearcher()
                bw_files = bw_searcher.find_files_with_keywords(
                    self.BW_SEARCH_DIR, 
                    [send_table]
                )
            
            # bw_files가 사전 형태이므로 send_table 키워드에 대한 결과를 가져옴
            matching_files = bw_files.get(send_table, [])
            
            # BW 쿼리 추출
            bw_queries = {
                'send': '',
                'recv': ''
            }
            extractor = BWQueryExtractor()
            for bw_file in matching_files:
                bw_file_path = os.path.join(self.BW_SEARCH_DIR, bw_file)
                if os.path.exists(bw_file_path):
                    queries = extractor.extract_bw_queries(bw_file_path)
                    
                    # 송신 쿼리가 없으면 첫 번째 송신 쿼리 저장
                    if not bw_queries['send'] and queries.get('send') and len(queries['send']) > 0:
                        bw_queries['send'] = queries['send'][0]
                    
                    # 수신 쿼리가 없으면 첫 번째 수신 쿼리 저장
                    if not bw_queries['recv'] and queries.get('recv') and len(queries['recv']) > 0:
                        bw_queries['recv'] = queries['recv'][0]
            
            # 결과 초기화
            comparisons = {
                'send': None,
                'recv': None
            }
            warnings = {
                'send': [],
                'recv': []
            }
            
            # 송신 쿼리 비교 (MQ XML vs BW XML)
            if file_results['send']['query'] and bw_queries['send']:
                try:
                    comparisons['send'] = self.query_parser.compare_queries(
                        file_results['send']['query'],
                        bw_queries['send']
                    )
                    warnings['send'].extend(
                        self.query_parser.check_special_columns(
                            file_results['send']['query'],
                            'send'
                        )
                    )
                except Exception as e:
                    print(f"Error comparing send queries: {e}")
                    print(f"MQ query: {file_results['send']['query']}")
                    print(f"BW query: {bw_queries['send']}")
            
            # 수신 쿼리 비교 (MQ XML vs BW XML)
            if file_results['recv']['query'] and bw_queries['recv']:
                try:
                    comparisons['recv'] = self.query_parser.compare_queries(
                        file_results['recv']['query'],
                        bw_queries['recv']
                    )
                    warnings['recv'].extend(
                        self.query_parser.check_special_columns(
                            file_results['recv']['query'],
                            'recv'
                        )
                    )
                except Exception as e:
                    print(f"Error comparing recv queries: {e}")
                    print(f"MQ query: {file_results['recv']['query']}")
                    print(f"BW query: {bw_queries['recv']}")
            
            # 결과 반환
            return {
                'interface_info': interface_info,
                'excel_results': excel_results,
                'file_results': file_results,
                'bw_queries': bw_queries,
                'comparisons': comparisons,
                'warnings': warnings,
                'bw_files': matching_files
            }
            
        except Exception as e:
            print(f"Error processing interface at column {start_col}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def process_all_interfaces_with_bw(self):
        """
        모든 인터페이스를 처리하고 BW 파일과 비교하여 엑셀 파일로 결과 저장
        """
        # 엑셀 파일 초기화
        self.initialize_excel_output()
        
        # 모든 열을 처리
        print("\n[인터페이스 처리 시작]")
        print("-" * 80)
        
        interface_count = 0
        processed_count = 0
        
        # B열부터 시작 (인터페이스는 2열부터 시작하도록 변경)
        col = 2
        
        # 인터페이스 블록 수 확인 (디버깅용)
        while True:
            if not self.worksheet.cell(row=2, column=col).value:
                break
            interface_count += 1
            col += 3
            
        if interface_count == 0:
            print("엑셀 파일에서 인터페이스 정보를 찾을 수 없습니다.")
            return
            
        print(f"엑셀 파일에서 총 {interface_count}개의 인터페이스를 찾았습니다.")
        print("-" * 80)
        
        # 실제 처리 시작
        interface_count = 0
        col = 2
        
        while True:
            # 인터페이스 ID 셀 확인
            if_id_cell = self.worksheet.cell(row=2, column=col)
            if not if_id_cell.value:
                # 더 이상 인터페이스가 없으면 종료
                break
            
            interface_count += 1
            print(f"\n처리 중: 인터페이스 #{interface_count} (열 {col})")
            print(f"인터페이스 ID: {if_id_cell.value}")
            
            # 인터페이스 처리 및 BW 파일 비교
            result = self.process_interface_with_bw(col)
            
            if result:
                processed_count += 1
                
                # 인터페이스 정보 출력
                interface_info = result['interface_info']
                print(f"인터페이스 ID: {interface_info['interface_id']}")
                print(f"인터페이스 명: {interface_info['interface_name']}")
                print(f"송신 시스템: {interface_info.get('send_system', 'N/A')}")
                print(f"수신 시스템: {interface_info.get('recv_system', 'N/A')}")
                print(f"송신 테이블: {interface_info.get('send_table', 'N/A')}")
                print(f"수신 테이블: {interface_info.get('recv_table', 'N/A')}")
                
                # BW 파일 정보 출력
                if result['bw_files']:
                    print(f"\nBW 파일 ({len(result['bw_files'])}개):")
                    for i, bw_file in enumerate(result['bw_files'], 1):
                        print(f"  {i}. {bw_file}")
                else:
                    print("\nBW 파일: 없음")
                
                # 송신 쿼리 비교 결과 출력
                print("\n[송신 SELECT 쿼리 비교]")
                if result['comparisons']['send']:
                    diff = result['comparisons']['send']
                    if not diff.is_equal:
                        print("※ 차이점 있음")
                        for difference in diff.differences:
                            print(f"- 컬럼: {difference['column']}")
                            print(f"  - Excel: {difference['query1_value']}")
                            print(f"  - 파일: {difference['query2_value']}")
                    else:
                        print("※ 차이점 없음")
                else:
                    print("※ 비교 결과 없음")
                
                # 수신 쿼리 비교 결과 출력
                print("\n[수신 INSERT 쿼리 비교]")
                if result['comparisons']['recv']:
                    diff = result['comparisons']['recv']
                    if not diff.is_equal:
                        print("※ 차이점 있음")
                        for difference in diff.differences:
                            print(f"- 컬럼: {difference['column']}")
                            print(f"  - Excel: {difference['query1_value']}")
                            print(f"  - 파일: {difference['query2_value']}")
                    else:
                        print("※ 차이점 없음")
                else:
                    print("※ 비교 결과 없음")
                
                # 각 인터페이스별 시트 생성
                self.create_interface_sheet(
                    interface_count,
                    interface_info,
                    result['file_results'],
                    result['comparisons'],
                    result['bw_queries']
                )
                
                # 결과를 리스트에 추가
                self.interface_results.append(result)
                
                print("-" * 80)
            else:
                print(f"인터페이스 #{interface_count} (열 {col}) 처리 실패")
            
            col += 3  # 다음 인터페이스 블록으로 이동
            
        # 처리 결과 출력
        print(f"\n총 인터페이스: {interface_count}개")
        print(f"처리된 인터페이스: {processed_count}개")
        
        # 엑셀 파일 저장
        if processed_count > 0:
            self.save_excel_output()
            print(f"\n엑셀 파일이 저장되었습니다: {self.output_path}")
        else:
            print("\n처리된 인터페이스가 없어 엑셀 파일을 생성하지 않았습니다.")

def main():
    # 고정된 경로 사용
    excel_path = 'C:\\work\\LT\\input_LT.xlsx' # 인터페이스 정보
    xml_dir = 'C:\\work\\LT\\xml' # MQ XML 파일 디렉토리
    bw_dir = 'C:\\work\\LT\\BW소스'  # BW XML 파일 디렉토리 경로
    
    # BW 검색 디렉토리 설정
    XMLComparator.BW_SEARCH_DIR = bw_dir
    
    comparator = XMLComparator(excel_path, xml_dir)
    
    # 명령행 인자가 있을 경우 처리
    if len(sys.argv) > 1:
        if sys.argv[1] == "excel":
            # 엑셀 출력 모드 실행
            print("\n[MQ XML과 BW XML 쿼리 비교 - 엑셀 출력 모드]")
            comparator.process_all_interfaces_with_bw()
            return
    
    # 기본 모드 실행 - 기존 로직 유지
    print("\n[MQ XML 파일 검색 및 쿼리 비교 시작]")
    comparator.process_all_interfaces()
    
    # BW 파일 검색 및 결과 출력을 마지막으로 이동
    print("\n[BW 파일 검색 시작]")
    bw_results = comparator.find_bw_files()
    comparator.print_bw_search_results(bw_results)
    
    # BW 파일에서 쿼리 추출
    print("\n[BW 파일 쿼리 추출]")
    print("-" * 80)
    extractor = BWQueryExtractor()  # BWQueryExtractor 사용
    for result in bw_results:
        if result['bw_files']:  # BW 파일이 있는 경우에만 처리
            print(f"\n인터페이스: {result['interface_name']} ({result['interface_id']})")
            print(f"송신 테이블: {result['send_table']}")
            print("찾은 BW 파일의 쿼리:")
            for bw_file in result['bw_files']:
                bw_file_path = os.path.join(bw_dir, bw_file)
                if os.path.exists(bw_file_path):
                    query = extractor.get_single_query(bw_file_path)  # BWQueryExtractor의 get_single_query 사용
                    if query:
                        print(f"\nBW 파일: {bw_file}")
                        print("-" * 40)
                        print(query)
                    else:
                        print(f"\nBW 파일: {bw_file} - 쿼리를 찾을 수 없음")

    print("\n[처리 완료]")
    print("엑셀 출력 모드로 실행하려면 'python comp_xml.py excel' 명령을 사용하세요.")

if __name__ == "__main__":
    main()