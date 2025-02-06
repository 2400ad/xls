import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from maptest import ColumnMapper

class MapperGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("DB 컬럼 매핑 도구")
        self.setup_gui()
        self.mapper = ColumnMapper()

    def setup_gui(self):
        # 프레임 생성
        send_frame = ttk.LabelFrame(self.root, text="송신 DB", padding="5")
        recv_frame = ttk.LabelFrame(self.root, text="수신 DB", padding="5")
        button_frame = ttk.Frame(self.root, padding="5")
        result_frame = ttk.LabelFrame(self.root, text="결과", padding="5")

        send_frame.pack(fill="x", padx=5, pady=5)
        recv_frame.pack(fill="x", padx=5, pady=5)
        button_frame.pack(fill="x", padx=5, pady=5)
        result_frame.pack(fill="both", expand=True, padx=5, pady=5)

        # 송신 DB 프레임
        ttk.Label(send_frame, text="SID:").grid(row=0, column=0, sticky="e")
        ttk.Label(send_frame, text="Username:").grid(row=1, column=0, sticky="e")
        ttk.Label(send_frame, text="Password:").grid(row=2, column=0, sticky="e")
        ttk.Label(send_frame, text="Owner:").grid(row=3, column=0, sticky="e")
        ttk.Label(send_frame, text="Table:").grid(row=4, column=0, sticky="e")
        ttk.Label(send_frame, text="컬럼 매핑:").grid(row=5, column=0, sticky="e")

        self.send_sid = ttk.Entry(send_frame)
        self.send_username = ttk.Entry(send_frame)
        self.send_password = ttk.Entry(send_frame, show="*")
        self.send_owner = ttk.Entry(send_frame)
        self.send_table = ttk.Entry(send_frame)
        self.send_columns = scrolledtext.ScrolledText(send_frame, width=30, height=10)

        self.send_sid.grid(row=0, column=1, sticky="ew")
        self.send_username.grid(row=1, column=1, sticky="ew")
        self.send_password.grid(row=2, column=1, sticky="ew")
        self.send_owner.grid(row=3, column=1, sticky="ew")
        self.send_table.grid(row=4, column=1, sticky="ew")
        self.send_columns.grid(row=5, column=1, sticky="ew")

        # 수신 DB 프레임
        ttk.Label(recv_frame, text="SID:").grid(row=0, column=0, sticky="e")
        ttk.Label(recv_frame, text="Username:").grid(row=1, column=0, sticky="e")
        ttk.Label(recv_frame, text="Password:").grid(row=2, column=0, sticky="e")
        ttk.Label(recv_frame, text="Owner:").grid(row=3, column=0, sticky="e")
        ttk.Label(recv_frame, text="Table:").grid(row=4, column=0, sticky="e")
        ttk.Label(recv_frame, text="컬럼 매핑:").grid(row=5, column=0, sticky="e")

        self.recv_sid = ttk.Entry(recv_frame)
        self.recv_username = ttk.Entry(recv_frame)
        self.recv_password = ttk.Entry(recv_frame, show="*")
        self.recv_owner = ttk.Entry(recv_frame)
        self.recv_table = ttk.Entry(recv_frame)
        self.recv_columns = scrolledtext.ScrolledText(recv_frame, width=30, height=10)

        self.recv_sid.grid(row=0, column=1, sticky="ew")
        self.recv_username.grid(row=1, column=1, sticky="ew")
        self.recv_password.grid(row=2, column=1, sticky="ew")
        self.recv_owner.grid(row=3, column=1, sticky="ew")
        self.recv_table.grid(row=4, column=1, sticky="ew")
        self.recv_columns.grid(row=5, column=1, sticky="ew")

        # 버튼 프레임
        ttk.Button(button_frame, text="실행", command=self.run_test).pack(side="left", padx=5)
        ttk.Button(button_frame, text="SQL 생성", command=self.generate_sql).pack(side="left", padx=5)
        ttk.Button(button_frame, text="XML 생성", command=self.generate_xml).pack(side="left", padx=5)

        # 결과 프레임
        self.result_text = scrolledtext.ScrolledText(result_frame, width=50, height=20)
        self.result_text.pack(fill="both", expand=True)

    def run_test(self):
        try:
            # 송신 DB 연결
            self.mapper.connect_send_db(
                self.send_sid.get(),
                self.send_username.get(),
                self.send_password.get()
            )

            # 수신 DB 연결
            self.mapper.connect_recv_db(
                self.recv_sid.get(),
                self.recv_username.get(),
                self.recv_password.get()
            )

            # 송신 테이블 설정
            self.mapper.set_send_table(
                self.send_owner.get(),
                self.send_table.get()
            )

            # 수신 테이블 설정
            self.mapper.set_recv_table(
                self.recv_owner.get(),
                self.recv_table.get()
            )

            # 매핑 컬럼 설정
            self.mapper.set_send_mapping(self.send_columns.get("1.0", tk.END))
            self.mapper.set_recv_mapping(self.recv_columns.get("1.0", tk.END))

            # 컬럼 비교 실행
            results = self.mapper.compare_columns()
            
            # 결과 표시
            self.result_text.delete("1.0", tk.END)
            
            has_error = False
            error_messages = ["컬럼 비교 결과:"]
            
            for result in results:
                if 'error' in result:
                    error_messages.append(f"\n{result['error']}")
                    has_error = True
                    continue
                    
                if result.get('errors'):
                    # 무시할 에러 메시지 목록
                    ignore_messages = [
                        "수신 매핑이 설정되지 않았습니다 (선택적 수신)",
                    ]
                    
                    # 실제 에러만 필터링
                    real_errors = [
                        error for error in result['errors']
                        if not any(ignore_msg in error for ignore_msg in ignore_messages)
                    ]
                    
                    # 실제 에러가 있는 경우만 출력
                    if real_errors:
                        error_messages.append(f"\n[{result['send_column']} -> {result['recv_column'] or '(매핑 없음)'}]")
                        error_messages.append("  - " + "\n  - ".join(real_errors))
                        has_error = True

            if not has_error:
                self.result_text.insert(tk.END, "모든 컬럼이 정상적으로 매핑되었습니다.")
            else:
                self.result_text.insert(tk.END, "\n".join(error_messages))

        except Exception as e:
            messagebox.showerror("오류", str(e))
        finally:
            self.mapper.close_connections()

    def generate_sql(self):
        try:
            # 송신 SQL 생성
            send_sql = self.mapper.generate_send_sql()
            recv_sql = self.mapper.generate_recv_sql()
            
            self.result_text.delete("1.0", tk.END)
            self.result_text.insert(tk.END, "=== 송신 SQL ===\n")
            self.result_text.insert(tk.END, send_sql)
            self.result_text.insert(tk.END, "\n\n=== 수신 SQL ===\n")
            self.result_text.insert(tk.END, recv_sql)

        except Exception as e:
            messagebox.showerror("오류", str(e))

    def generate_xml(self):
        try:
            xml = self.mapper.generate_field_xml()
            self.result_text.delete("1.0", tk.END)
            self.result_text.insert(tk.END, xml)
        except Exception as e:
            messagebox.showerror("오류", str(e))

    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    app = MapperGUI()
    app.run()