import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import os
from typing import Dict, List, Optional, Tuple
import datetime
import ast


def read_interface_block(ws, start_col):
    """Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다.
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
        try:
            send_db_value = ws.cell(row=3, column=start_col).value
            send_db_info = ast.literal_eval(send_db_value) if send_db_value else {}
            
            recv_db_value = ws.cell(row=3, column=start_col + 1).value
            recv_db_info = ast.literal_eval(recv_db_value) if recv_db_value else {}
        except (SyntaxError, ValueError):
            # 데이터 형식 오류 시 빈 딕셔너리로 설정
            send_db_info = {}
            recv_db_info = {}
            
        interface_info['send']['db_info'] = send_db_info
        interface_info['recv']['db_info'] = recv_db_info
        
        # 테이블 정보 (4행에서 읽기)
        try:
            send_table_value = ws.cell(row=4, column=start_col).value
            send_table_info = ast.literal_eval(send_table_value) if send_table_value else {}
            
            recv_table_value = ws.cell(row=4, column=start_col + 1).value
            recv_table_info = ast.literal_eval(recv_table_value) if recv_table_value else {}
        except (SyntaxError, ValueError):
            # 데이터 형식 오류 시 빈 딕셔너리로 설정
            send_table_info = {}
            recv_table_info = {}
        
        interface_info['send']['owner'] = send_table_info.get('owner')
        interface_info['send']['table_name'] = send_table_info.get('table_name')
        interface_info['recv']['owner'] = recv_table_info.get('owner')
        interface_info['recv']['table_name'] = recv_table_info.get('table_name')
        
        # 컬럼 매핑 정보 (5행부터)
        row = 5
        while True:
            send_col = ws.cell(row=row, column=start_col).value
            recv_col = ws.cell(row=row, column=start_col + 1).value
            
            # 둘 다 None이면 컬럼 매핑 끝
            if send_col is None and recv_col is None:
                break
                
            # 송신 컬럼 추가
            if send_col:
                interface_info['send']['columns'].append(send_col)
                
            # 수신 컬럼 추가
            if recv_col:
                interface_info['recv']['columns'].append(recv_col)
                
            row += 1
        
        return interface_info
        
    except Exception as e:
        print(f"인터페이스 정보 읽기 오류: {e}")
        return None


class ExcelManager:
    """
    Excel 파일 관리 및 출력을 위한 클래스
    """

    def __init__(self, excel_path: str = None):
        """
        Excel 관리자 클래스 초기화
        
        Args:
            excel_path (str, optional): 기존 엑셀 파일 경로 (없으면 새로 생성)
        """
        self.excel_path = excel_path
        if excel_path and os.path.exists(excel_path):
            self.workbook = openpyxl.load_workbook(excel_path)
        else:
            self.workbook = openpyxl.Workbook()
        
        self.output_path = ''
    
    def initialize_excel_output(self):
        """
        결과를 저장할 새 엑셀 파일 초기화
        
        Returns:
            openpyxl.worksheet.worksheet.Worksheet: 생성된 시트 객체
        """
        # 기존 시트가 있으면 모두 삭제
        for sheet_name in self.workbook.sheetnames:
            del self.workbook[sheet_name]
        
        # 요약 시트 생성
        sheet = self.workbook.create_sheet("요약")
        
        # 제목 행 스타일 정의
        header_fill = PatternFill(start_color="CCCCFF", end_color="CCCCFF", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        align_left = Alignment(horizontal='left', vertical='center', wrap_text=True)
        wrap_text_top = Alignment(wrap_text=True, vertical='top')
        
        # 열 너비 설정
        sheet.column_dimensions['A'].width = 5   # 일련번호
        sheet.column_dimensions['B'].width = 15  # 인터페이스 ID
        sheet.column_dimensions['C'].width = 15  # 인터페이스 명
        sheet.column_dimensions['D'].width = 15  # 송신 테이블
        sheet.column_dimensions['E'].width = 15  # MQ 송신 파일
        sheet.column_dimensions['F'].width = 15  # BW 송신 파일
        sheet.column_dimensions['G'].width = 15  # 송신 비교 결과
        sheet.column_dimensions['H'].width = 15  # MQ 수신 파일
        sheet.column_dimensions['I'].width = 15  # BW 수신 파일
        sheet.column_dimensions['J'].width = 15  # 수신 비교 결과
        
        # 헤더 행 생성
        headers = ["번호", "인터페이스 ID", "인터페이스 명", "송신 테이블", "MQ 송신 파일", "BW 송신 파일", "송신 비교 결과", 
                  "MQ 수신 파일", "BW 수신 파일", "수신 비교 결과"]
        
        for col_idx, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col_idx, value=header)
            cell.font = Font(bold=True, size=10)
            cell.fill = header_fill
            cell.alignment = align_center
            cell.border = border
        
        return sheet
    
    def update_summary_sheet(self, data, row=None):
        """
        요약 시트에 인터페이스 정보 추가
        
        Args:
            data (dict): 인터페이스 데이터
            row (int, optional): 추가할 행 번호 (None이면 마지막 행 다음에 추가)
        """
        sheet = self.workbook["요약"] if "요약" in self.workbook.sheetnames else self.workbook.active
        
        # 추가할 행 번호 결정
        if row is None:
            # 마지막 행 다음에 추가
            row = 2
            for r in range(2, sheet.max_row + 2):
                if sheet.cell(row=r, column=2).value is None:
                    row = r
                    break
        
        # 일련번호 계산
        seq_num = row - 1
        seq_num_formatted = f"{seq_num:02d}"  # 01, 02, ... 형식으로 포맷팅
        
        # 인터페이스 정보
        interface_info = data.get("interface_info", {})
        file_results = data.get("file_results", {})
        bw_files = data.get("bw_files", [])
        comparisons = data.get("comparisons", {})
        
        # 값 설정
        sheet.cell(row=row, column=1, value=seq_num_formatted)  # 일련번호
        sheet.cell(row=row, column=2, value=interface_info.get("interface_id", ""))
        sheet.cell(row=row, column=3, value=interface_info.get("interface_name", ""))
        sheet.cell(row=row, column=4, value=f"{interface_info.get('send', {}).get('owner', '')}.{interface_info.get('send', {}).get('table_name', '')}")
        
        # MQ 파일 정보
        sheet.cell(row=row, column=5, value=file_results.get("send", {}).get("path", ""))
        
        # BW 파일 정보
        if isinstance(bw_files, list) and len(bw_files) > 0:
            sheet.cell(row=row, column=6, value=bw_files[0])
        elif isinstance(bw_files, dict):
            sheet.cell(row=row, column=6, value=bw_files.get("send", ""))
        
        # 비교 결과 - 송신
        send_comparison = comparisons.get("send", {})
        if isinstance(send_comparison, dict):
            is_equal = send_comparison.get("is_equal", False)
            sheet.cell(row=row, column=7, value="일치" if is_equal else "차이")
        else:
            sheet.cell(row=row, column=7, value="비교불가")
        
        # MQ 수신 파일
        sheet.cell(row=row, column=8, value=file_results.get("recv", {}).get("path", ""))
        
        # BW 수신 파일
        if isinstance(bw_files, list) and len(bw_files) > 1:
            sheet.cell(row=row, column=9, value=bw_files[1])
        elif isinstance(bw_files, dict):
            sheet.cell(row=row, column=9, value=bw_files.get("recv", ""))
        
        # 비교 결과 - 수신
        recv_comparison = comparisons.get("recv", {})
        if isinstance(recv_comparison, dict):
            is_equal = recv_comparison.get("is_equal", False)
            sheet.cell(row=row, column=10, value="일치" if is_equal else "차이")
        else:
            sheet.cell(row=row, column=10, value="비교불가")
    
    def save_excel_output(self, output_path):
        """
        처리된 결과를 엑셀 파일로 저장
        
        Args:
            output_path (str): 출력 엑셀 파일 경로
            
        Returns:
            bool: 저장 성공 여부
        """
        # output_path 값을 인스턴스 변수에 저장
        self.output_path = output_path
        
        try:
            self.workbook.save(output_path)
            print(f"\n[결과 저장 완료] 파일 경로: {output_path}")
            return True
        except Exception as e:
            print(f"엑셀 파일 저장 중 오류 발생: {e}")
            return False
    
    def create_interface_sheet(self, if_info, mq_files=None, bw_files=None, queries=None, comparison_results=None):
        """
        엑셀 파일에 각 인터페이스별 시트를 생성하고, 데이터를 기록
        
        Args:
            if_info (dict): 인터페이스 정보
            mq_files (dict): MQ 파일 정보 (송신/수신)
            bw_files (dict): BW 파일 정보 (송신/수신)
            queries (dict): 쿼리 정보 (MQ/BW, 송신/수신)
            comparison_results (dict): 비교 결과
        """
        if mq_files is None:
            mq_files = {}
        if bw_files is None:
            bw_files = {}
        if queries is None:
            queries = {}
        if comparison_results is None:
            comparison_results = {}
        
        # 시트 이름 생성 (인터페이스 이름 또는 ID)
        sheet_name = if_info.get('interface_name', '') or if_info.get('interface_id', '')
        
        # 일련번호 찾기
        seq_num = '01'  # 기본값
        if "요약" in self.workbook.sheetnames:
            summary_sheet = self.workbook["요약"]
            for row in range(2, summary_sheet.max_row + 1):
                interface_id = summary_sheet.cell(row=row, column=2).value
                if interface_id == if_info.get('interface_id', ''):
                    seq_num = summary_sheet.cell(row=row, column=1).value
                    break
        
        # 시트 이름 앞에 일련번호 추가
        sheet_name = f"{seq_num}_{sheet_name}"
        
        # 시트 이름이 30자를 초과하면 자르기 (Excel 시트 이름 제한)
        if len(sheet_name) > 30:
            sheet_name = sheet_name[:27] + '...'
        
        # 시트 이름이 중복되는 경우 처리
        base_name = sheet_name
        counter = 1
        while sheet_name in self.workbook.sheetnames:
            sheet_name = f"{base_name[:25]}_{counter}"
            counter += 1
        
        # 인터페이스 시트 생성
        sheet = self.workbook.create_sheet(title=sheet_name)
        
        # 스타일 정의
        header_fill = PatternFill(start_color="E6E6FA", end_color="E6E6FA", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        align_left = Alignment(horizontal='left', vertical='center', wrap_text=True)
        wrap_text_top = Alignment(wrap_text=True, vertical='top')
        
        # 열 너비 설정 - 모든 열을 A열과 동일하게 설정하고 2배 크기로 확장
        column_width = 30  # A열의 기본 너비의 2배
        for col_letter in ['A', 'B', 'C', 'D', 'F']:
            sheet.column_dimensions[col_letter].width = column_width
        
        # E열은 다른 열의 절반 크기로 설정
        sheet.column_dimensions['E'].width = column_width / 2
        
        # 기본 글꼴 크기 설정
        font_size_normal = 10
        font_size_query = 9
        
        # 1. 인터페이스 정보 헤더 설정
        row = 1
        sheet.cell(row=row, column=1, value="인터페이스 정보").font = Font(bold=True, size=font_size_normal)
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=5)
        sheet.cell(row=row, column=1).fill = header_fill
        sheet.cell(row=row, column=1).alignment = align_center
        
        # 인터페이스 ID 및 이름
        row = 2
        sheet.cell(row=row, column=1, value="인터페이스 ID").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=2, value=if_info.get('interface_id', ''))
        sheet.cell(row=row, column=3, value="인터페이스 명").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=4, value=if_info.get('interface_name', ''))
        sheet.merge_cells(start_row=row, start_column=4, end_row=row, end_column=5)
        
        # 송신 시스템 정보
        row = 3
        send_db_info = if_info.get('send', {}).get('db_info', {})
        send_sid = ''
        if isinstance(send_db_info, dict) and 'sid' in send_db_info:
            # sid에서 ip:port 형식 제외
            sid_full = send_db_info['sid']
            if isinstance(sid_full, str) and ':' in sid_full:
                # ip:port/sid 형식이면 sid만 추출
                send_sid = sid_full.split('/')[-1] if '/' in sid_full else sid_full
            else:
                send_sid = sid_full
                
        sheet.cell(row=row, column=1, value="송신 시스템").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=2, value=send_sid)
        sheet.cell(row=row, column=3, value="송신 테이블").font = Font(bold=True, size=font_size_normal)
        
        send_owner = if_info.get('send', {}).get('owner', '')
        send_table = if_info.get('send', {}).get('table_name', '')
        full_table_name = f"{send_owner}.{send_table}" if send_owner and send_table else send_table
        
        sheet.cell(row=row, column=4, value=full_table_name)
        sheet.merge_cells(start_row=row, start_column=4, end_row=row, end_column=5)
        
        # 수신 시스템 정보
        row = 4
        recv_db_info = if_info.get('recv', {}).get('db_info', {})
        recv_sid = ''
        if isinstance(recv_db_info, dict) and 'sid' in recv_db_info:
            # sid에서 ip:port 형식 제외
            sid_full = recv_db_info['sid']
            if isinstance(sid_full, str) and ':' in sid_full:
                # ip:port/sid 형식이면 sid만 추출
                recv_sid = sid_full.split('/')[-1] if '/' in sid_full else sid_full
            else:
                recv_sid = sid_full
                
        sheet.cell(row=row, column=1, value="수신 시스템").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=2, value=recv_sid)
        sheet.cell(row=row, column=3, value="수신 테이블").font = Font(bold=True, size=font_size_normal)
        
        recv_owner = if_info.get('recv', {}).get('owner', '')
        recv_table = if_info.get('recv', {}).get('table_name', '')
        full_recv_table = f"{recv_owner}.{recv_table}" if recv_owner and recv_table else recv_table
        
        sheet.cell(row=row, column=4, value=full_recv_table)
        sheet.merge_cells(start_row=row, start_column=4, end_row=row, end_column=5)
        
        # 2. 파일 정보 및 쿼리 비교 섹션
        row = 6
        
        # 2.1 송신 파일 정보
        sheet.cell(row=row, column=1, value="송신 파일명").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=1).fill = header_fill
        
        sheet.cell(row=row, column=2, value="MQ 송신 파일").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=2).fill = header_fill
        sheet.cell(row=row, column=2).alignment = align_center
        
        sheet.cell(row=row, column=4, value="BW 송신 파일").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=4).fill = header_fill
        sheet.cell(row=row, column=4).alignment = align_center
        
        # 송신 파일 정보 입력
        row = 7
        sheet.cell(row=row, column=2, value=mq_files.get('send', {}).get('path', 'N/A'))
        if isinstance(bw_files, dict):
            sheet.cell(row=row, column=4, value=bw_files.get('send', 'N/A'))
        else:
            sheet.cell(row=row, column=4, value='N/A')
        
        # 2.2 수신 파일 정보
        row = 8
        sheet.cell(row=row, column=1, value="수신 파일명").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=1).fill = header_fill
        
        sheet.cell(row=row, column=2, value="MQ 수신 파일").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=2).fill = header_fill
        sheet.cell(row=row, column=2).alignment = align_center
        
        sheet.cell(row=row, column=4, value="BW 수신 파일").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=4).fill = header_fill
        sheet.cell(row=row, column=4).alignment = align_center
        
        # 수신 파일 정보 입력
        row = 9
        sheet.cell(row=row, column=2, value=mq_files.get('recv', {}).get('path', 'N/A'))
        if isinstance(bw_files, dict):
            sheet.cell(row=row, column=4, value=bw_files.get('recv', 'N/A'))
        else:
            sheet.cell(row=row, column=4, value='N/A')
        
        # 2.3 송신 쿼리 및 비교 결과
        row = 11
        sheet.cell(row=row, column=1, value="송신 MQ 쿼리").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=1).fill = header_fill
        sheet.cell(row=row, column=1).alignment = align_center
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
        
        sheet.cell(row=row, column=3, value="송신 BW 쿼리").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=3).fill = header_fill
        sheet.cell(row=row, column=3).alignment = align_center
        sheet.merge_cells(start_row=row, start_column=3, end_row=row, end_column=4)
        
        sheet.cell(row=row, column=5, value="비교").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=5).fill = header_fill
        sheet.cell(row=row, column=5).alignment = align_center
        
        sheet.cell(row=row, column=6, value="비교 상세").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=6).fill = header_fill
        sheet.cell(row=row, column=6).alignment = align_center
        
        # 송신 쿼리 정보 입력
        row = 12
        sheet.cell(row=row, column=1, value=mq_files.get('send', {}).get('query', queries.get('mq_send', 'N/A')))
        sheet.cell(row=row, column=1).alignment = wrap_text_top
        sheet.cell(row=row, column=1).font = Font(size=font_size_query)
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
        
        sheet.cell(row=row, column=3, value=queries.get('bw_send', 'N/A'))
        sheet.cell(row=row, column=3).alignment = wrap_text_top
        sheet.cell(row=row, column=3).font = Font(size=font_size_query)
        sheet.merge_cells(start_row=row, start_column=3, end_row=row, end_column=4)
        
        # 비교 결과 입력
        is_send_equal = False
        send_detail = 'N/A'
        if comparison_results and 'send' in comparison_results:
            if isinstance(comparison_results['send'], dict):
                is_send_equal = comparison_results['send'].get('is_equal', False)
                send_detail = comparison_results['send'].get('detail', 'N/A')
            elif hasattr(comparison_results['send'], 'is_equal'):
                is_send_equal = comparison_results['send'].is_equal
                send_detail = getattr(comparison_results['send'], 'detail', 'N/A')
                
        sheet.cell(row=row, column=5, value='일치' if is_send_equal else '차이')
        sheet.cell(row=row, column=5).alignment = align_center
        sheet.cell(row=row, column=6, value=send_detail)
        sheet.cell(row=row, column=6).alignment = wrap_text_top
        
        if is_send_equal:
            sheet.cell(row=row, column=5).fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        else:
            sheet.cell(row=row, column=5).fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        
        # 쿼리 행 높이 설정
        sheet.row_dimensions[row].height = 150
        
        # 2.4 수신 쿼리 및 비교 결과
        row = 14
        sheet.cell(row=row, column=1, value="수신 MQ 쿼리").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=1).fill = header_fill
        sheet.cell(row=row, column=1).alignment = align_center
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
        
        sheet.cell(row=row, column=3, value="수신 BW 쿼리").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=3).fill = header_fill
        sheet.cell(row=row, column=3).alignment = align_center
        sheet.merge_cells(start_row=row, start_column=3, end_row=row, end_column=4)
        
        sheet.cell(row=row, column=5, value="비교").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=5).fill = header_fill
        sheet.cell(row=row, column=5).alignment = align_center
        
        sheet.cell(row=row, column=6, value="비교 상세").font = Font(bold=True, size=font_size_normal)
        sheet.cell(row=row, column=6).fill = header_fill
        sheet.cell(row=row, column=6).alignment = align_center
        
        # 수신 쿼리 정보 입력
        row = 15
        sheet.cell(row=row, column=1, value=mq_files.get('recv', {}).get('query', queries.get('mq_recv', 'N/A')))
        sheet.cell(row=row, column=1).alignment = wrap_text_top
        sheet.cell(row=row, column=1).font = Font(size=font_size_query)
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
        
        sheet.cell(row=row, column=3, value=queries.get('bw_recv', 'N/A'))
        sheet.cell(row=row, column=3).alignment = wrap_text_top
        sheet.cell(row=row, column=3).font = Font(size=font_size_query)
        sheet.merge_cells(start_row=row, start_column=3, end_row=row, end_column=4)
        
        # 비교 결과 입력
        is_recv_equal = False
        recv_detail = 'N/A'
        if comparison_results and 'recv' in comparison_results:
            if isinstance(comparison_results['recv'], dict):
                is_recv_equal = comparison_results['recv'].get('is_equal', False)
                recv_detail = comparison_results['recv'].get('detail', 'N/A')
            elif hasattr(comparison_results['recv'], 'is_equal'):
                is_recv_equal = comparison_results['recv'].is_equal
                recv_detail = getattr(comparison_results['recv'], 'detail', 'N/A')
                
        sheet.cell(row=row, column=5, value='일치' if is_recv_equal else '차이')
        sheet.cell(row=row, column=5).alignment = align_center
        sheet.cell(row=row, column=6, value=recv_detail)
        sheet.cell(row=row, column=6).alignment = wrap_text_top
        
        if is_recv_equal:
            sheet.cell(row=row, column=5).fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        else:
            sheet.cell(row=row, column=5).fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        
        # 쿼리 행 높이 설정
        sheet.row_dimensions[row].height = 150
        
        # 모든 셀에 테두리 적용
        for row_cells in sheet.iter_rows(min_row=1, max_row=15, min_col=1, max_col=6):
            for cell in row_cells:
                cell.border = border
        
        return sheet
    
    def close(self):
        """
        리소스 정리
        """
        if self.workbook:
            if self.output_path:
                try:
                    self.workbook.save(self.output_path)
                except:
                    pass


# 간단한 사용 예시
def main():
    # 예시 코드
    excel_manager = ExcelManager()
    sheet = excel_manager.initialize_excel_output()
    
    # 샘플 데이터로 요약 시트 업데이트
    sample_data = {
        "interface_info": {
            "interface_id": "IF001", 
            "interface_name": "테스트 인터페이스", 
            "send": {
                "owner": "OWNER",
                "table_name": "TEST_TABLE",
                "db_info": {
                    "sid": "10.10.10.10:1521/DEVDB",
                    "system": "개발시스템"
                }
            },
            "recv": {
                "owner": "OWNER",
                "table_name": "TEST_RCV_TABLE",
                "db_info": {
                    "sid": "10.10.10.20:1521/PRODDB",
                    "system": "운영시스템"
                }
            }
        }, 
        "file_results": {
            "send": {"path": "test.SND.xml", "query": "SELECT * FROM OWNER.TEST_TABLE"}, 
            "recv": {"path": "test.RCV.xml", "query": "SELECT * FROM OWNER.TEST_RCV_TABLE"}
        }, 
        "bw_files": ["bw_mapping.xml", "bw_mapping2.xml"], 
        "bw_queries": {
            "send": "INSERT INTO OWNER.TEST_TABLE VALUES (:1, :2, :3)",
            "recv": "INSERT INTO OWNER.TEST_RCV_TABLE VALUES (:1, :2, :3)"
        },
        "comparisons": {
            "send": {"is_equal": True, "detail": "일치 - 테이블과 칼럼이 모두 동일합니다."}, 
            "recv": {"is_equal": False, "detail": "차이 - MQ 쿼리는 모든 컬럼을 선택하지만 BW 쿼리는 특정 컬럼만 선택합니다."}
        }
    }
    excel_manager.update_summary_sheet(sample_data)
    
    # 샘플 인터페이스 시트 생성
    if_info = {
        'interface_id': 'IF001',
        'interface_name': '테스트 인터페이스',
        'send': {
            'owner': 'OWNER',
            'table_name': 'TEST_TABLE',
            'db_info': {
                'sid': '10.10.10.10:1521/DEVDB',
                'system': '개발시스템'
            }
        },
        'recv': {
            'owner': 'OWNER',
            'table_name': 'TEST_RCV_TABLE',
            'db_info': {
                'sid': '10.10.10.20:1521/PRODDB',
                'system': '운영시스템'
            }
        }
    }
    
    mq_files = {
        'send': {'path': 'test.SND.xml', 'query': 'SELECT * FROM OWNER.TEST_TABLE WHERE 1=1 AND col1 = :value1 AND col2 = :value2'},
        'recv': {'path': 'test.RCV.xml', 'query': 'SELECT * FROM OWNER.TEST_RCV_TABLE WHERE status = \'Y\''}
    }
    
    bw_files = {
        'send': 'bw_mapping.xml',
        'recv': 'bw_mapping2.xml'
    }
    
    queries = {
        'mq_send': 'SELECT * FROM OWNER.TEST_TABLE WHERE 1=1 AND col1 = :value1 AND col2 = :value2',
        'bw_send': 'INSERT INTO OWNER.TEST_TABLE (col1, col2, col3) VALUES (:1, :2, :3)',
        'mq_recv': 'SELECT * FROM OWNER.TEST_RCV_TABLE WHERE status = \'Y\'',
        'bw_recv': 'INSERT INTO OWNER.TEST_RCV_TABLE (col1, col2, status) VALUES (:1, :2, \'Y\')'
    }
    
    comparison_results = {
        'send': {
            'is_equal': True, 
            'detail': '일치 - 테이블과 칼럼이 모두 동일합니다.'
        },
        'recv': {
            'is_equal': False, 
            'detail': '차이 - MQ 쿼리는 모든 컬럼을 선택하지만 BW 쿼리는 특정 컬럼만 선택합니다.\n- MQ: status = \'Y\'\n- BW: VALUES 값에 직접 \'Y\' 사용'
        }
    }
    
    excel_manager.create_interface_sheet(if_info, mq_files, bw_files, queries, comparison_results)
    
    # 결과 저장
    output_path = 'comp_mq_bw_sample.xlsx'
    excel_manager.save_excel_output(output_path)
    print(f"결과가 {output_path}에 저장되었습니다.")
    
    excel_manager.close()


if __name__ == "__main__":
    main()