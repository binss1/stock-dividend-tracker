import os
import pandas as pd
import sqlite3
import yfinance as yf
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import time

# 기본 설정
DB_FILE = 'data/stock_portfolio.db'
CSV_FILE = 'data/해외주식잔고현황.csv'
os.makedirs('data', exist_ok=True)
os.makedirs('reports', exist_ok=True)

def create_database():
    """SQLite 데이터베이스 생성"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 주식 보유 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_holdings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market TEXT,
        ticker TEXT NOT NULL,
        company_name TEXT NOT NULL,
        shares INTEGER NOT NULL,
        purchase_price REAL NOT NULL,
        current_price REAL,
        total_value REAL,
        profit_loss_percent REAL,
        profit_loss_amount REAL,
        purchase_amount REAL,
        currency TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 배당금 정보 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dividend_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL UNIQUE,
        company_name TEXT,
        ex_dividend_date TEXT,
        payment_date TEXT,
        dividend_amount REAL,
        dividend_yield REAL,
        frequency TEXT,
        annual_dividend REAL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()
    print("데이터베이스가 성공적으로 생성되었습니다.")

def load_csv_data():
    """CSV 파일에서 주식 데이터 로드"""
    if not os.path.exists(CSV_FILE):
        print(f"CSV 파일을 찾을 수 없습니다: {CSV_FILE}")
        print("샘플 데이터를 사용합니다.")
        
        # 샘플 데이터 생성
        sample_data = [
            {'market': 'NYSE', 'ticker': 'AAPL', 'company_name': 'Apple Inc.', 'shares': 10, 'purchase_price': 150.0},
            {'market': 'NASDAQ', 'ticker': 'MSFT', 'company_name': 'Microsoft Corporation', 'shares': 5, 'purchase_price': 250.0},
            {'market': 'NYSE', 'ticker': 'JNJ', 'company_name': 'Johnson & Johnson', 'shares': 8, 'purchase_price': 160.0},
            {'market': 'NYSE', 'ticker': 'PG', 'company_name': 'Procter & Gamble Co.', 'shares': 15, 'purchase_price': 140.0},
            {'market': 'NYSE', 'ticker': 'KO', 'company_name': 'Coca-Cola Company', 'shares': 20, 'purchase_price': 55.0}
        ]
        
        df = pd.DataFrame(sample_data)
        return df
    
    try:
        # 여러 인코딩 시도
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(CSV_FILE, encoding=encoding)
                print(f"CSV 파일을 {encoding} 인코딩으로 성공적으로 읽었습니다.")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            print("모든 인코딩 방식으로 파일을 열 수 없습니다. 샘플 데이터를 사용합니다.")
            return load_csv_data()
        
        # 컬럼명 확인
        print("CSV 파일의 컬럼명:", df.columns.tolist())
        
        # 한글 인코딩 문제로 인한 컬럼 매핑
        column_mapping = {
            df.columns[0]: 'market',  # 시장구분
            df.columns[1]: 'ticker',  # 종목코드
            df.columns[2]: 'company_name',  # 종목명
            df.columns[3]: 'shares',  # 잔고수량
            df.columns[5]: 'purchase_price',  # 매수단가
            df.columns[6]: 'current_price',  # 현재가
            df.columns[7]: 'total_value',  # 평가금액
            df.columns[8]: 'profit_loss_percent',  # 수익률
            df.columns[9]: 'profit_loss_amount',  # 평가손익
            df.columns[10]: 'purchase_amount',  # 매수금액
            df.columns[12]: 'currency'  # 통화구분
        }
        
        # 열 이름 변경
        df = df.rename(columns=column_mapping)
        
        # 필요한 열만 추출
        cols_to_use = ['market', 'ticker', 'company_name', 'shares', 'purchase_price']
        
        df = df[cols_to_use]
        
        # 데이터 정리 (쉼표 제거, 퍼센트 기호 제거 등)
        for col in ['shares', 'purchase_price']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '').astype(float)
        
        return df
        
    except Exception as e:
        print(f"CSV 파일 처리 오류: {e}")
        print("샘플 데이터를 사용합니다.")
        return load_csv_data()

def update_stock_data(df):
    """주식 데이터 SQLite 업데이트"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 기존 데이터 삭제
    cursor.execute("DELETE FROM stock_holdings")
    
    # 데이터 삽입
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT INTO stock_holdings 
        (market, ticker, company_name, shares, purchase_price)
        VALUES (?, ?, ?, ?, ?)
        """, (
            row['market'],
            row['ticker'],
            row['company_name'],
            row['shares'],
            row['purchase_price']
        ))
    
    conn.commit()
    
    # 티커 목록 가져오기
    tickers = df['ticker'].tolist()
    
    # 티커를 작은 배치로 나누기 (한 번에 5개씩)
    batch_size = 5
    ticker_batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    
    success_count = 0
    
    for batch in ticker_batches:
        try:
            print(f"티커 배치 처리 중: {batch}")
            
            # 각 티커를 개별적으로 처리
            for ticker in batch:
                try:
                    # 단일 티커 처리
                    ticker_obj = yf.Ticker(ticker)
                    hist = ticker_obj.history(period="1d")
                    
                    if hist.empty:
                        print(f"{ticker}: 데이터를 가져올 수 없습니다.")
                        continue
                    
                    current_price = hist['Close'].iloc[-1]
                    
                    # 주식 보유 테이블 업데이트
                    cursor.execute("""
                    UPDATE stock_holdings
                    SET current_price = ?,
                        total_value = shares * ?,
                        profit_loss_amount = shares * (? - purchase_price),
                        profit_loss_percent = CASE WHEN purchase_price = 0 THEN 0 
                                             ELSE ((? - purchase_price) / purchase_price) * 100 END,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = ?
                    """, (
                        float(current_price),
                        float(current_price),
                        float(current_price),
                        float(current_price),
                        ticker
                    ))
                    
                    # 배당금 정보 처리
                    try:
                        info = ticker_obj.info
                        
                        # 배당 정보가 있는 경우에만 처리
                        if 'dividendRate' in info and info['dividendRate']:
                            dividend_data = {
                                'ticker': ticker,
                                'company_name': info.get('longName', ''),
                                'dividend_amount': info.get('lastDividendValue', 0) or info.get('dividendRate', 0) / 4,
                                'dividend_yield': info.get('dividendYield', 0) * 100 if info.get('dividendYield') else 0,
                                'annual_dividend': info.get('dividendRate', 0)
                            }
                            
                            # 배당 주기 추정
                            div_rate = info.get('dividendRate', 0)
                            div_value = info.get('lastDividendValue', 0)
                            
                            if div_rate and div_value:
                                ratio = round(div_rate / div_value) if div_value > 0 else 0
                                
                                if ratio == 4:
                                    dividend_data['frequency'] = 'Quarterly'
                                elif ratio == 2:
                                    dividend_data['frequency'] = 'Semi-Annual'
                                elif ratio == 12:
                                    dividend_data['frequency'] = 'Monthly'
                                elif ratio == 1:
                                    dividend_data['frequency'] = 'Annual'
                                else:
                                    dividend_data['frequency'] = 'Irregular'
                            else:
                                dividend_data['frequency'] = 'Unknown'
                            
                            # 배당금 정보 삽입/업데이트
                            cursor.execute("""
                            INSERT OR REPLACE INTO dividend_data 
                            (ticker, company_name, dividend_amount, dividend_yield, frequency, annual_dividend, last_updated)
                            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                            """, (
                                dividend_data['ticker'],
                                dividend_data['company_name'],
                                dividend_data['dividend_amount'],
                                dividend_data['dividend_yield'],
                                dividend_data['frequency'],
                                dividend_data['annual_dividend']
                            ))
                            
                            print(f"{ticker} 배당금 정보가 업데이트되었습니다.")
                        
                    except Exception as e:
                        print(f"{ticker} 배당금 정보 업데이트 오류: {e}")
                    
                    success_count += 1
                    print(f"{ticker} 데이터가 성공적으로 업데이트되었습니다.")
                    
                except Exception as e:
                    print(f"{ticker} 데이터 업데이트 오류: {e}")
            
            conn.commit()
            # 배치 사이 잠시 대기하여 API 제한 회피
            time.sleep(1)
            
        except Exception as e:
            print(f"배치 처리 중 오류 발생: {e}")
            continue
    
    # 샘플 데이터 추가 (테스트를 위해 실제 데이터를 가져오지 못한 경우)
    if success_count == 0:
        print("실제 데이터를 가져오지 못했습니다. 샘플 데이터를 추가합니다.")
        sample_stocks = [
            ('NYSE', 'AAPL', 'Apple Inc.', 10, 150.0, 175.0),
            ('NASDAQ', 'MSFT', 'Microsoft Corporation', 5, 250.0, 280.0),
            ('NYSE', 'JNJ', 'Johnson & Johnson', 8, 160.0, 155.0),
            ('NYSE', 'PG', 'Procter & Gamble Co.', 15, 140.0, 145.0),
            ('NYSE', 'KO', 'Coca-Cola Company', 20, 55.0, 58.0)
        ]
        
        for stock in sample_stocks:
            cursor.execute("""
            INSERT INTO stock_holdings 
            (market, ticker, company_name, shares, purchase_price, current_price,
             total_value, profit_loss_amount, profit_loss_percent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stock[0], stock[1], stock[2], stock[3], stock[4], stock[5],
                stock[3] * stock[5],
                stock[3] * (stock[5] - stock[4]),
                ((stock[5] - stock[4]) / stock[4]) * 100
            ))
            
            # 샘플 배당금 데이터
            if stock[1] in ['AAPL', 'MSFT', 'JNJ', 'PG', 'KO']:
                div_yield = 0.0
                annual_div = 0.0
                frequency = 'Quarterly'
                
                if stock[1] == 'AAPL':
                    div_yield = 0.5
                    annual_div = 0.88
                elif stock[1] == 'MSFT':
                    div_yield = 0.8
                    annual_div = 2.24
                elif stock[1] == 'JNJ':
                    div_yield = 2.7
                    annual_div = 4.19
                elif stock[1] == 'PG':
                    div_yield = 2.4
                    annual_div = 3.48
                elif stock[1] == 'KO':
                    div_yield = 2.9
                    annual_div = 1.68
                    
                cursor.execute("""
                INSERT OR REPLACE INTO dividend_data 
                (ticker, company_name, dividend_amount, dividend_yield, frequency, annual_dividend)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    stock[1],
                    stock[2],
                    annual_div / 4,  # 분기 배당금
                    div_yield,
                    frequency,
                    annual_div
                ))
        
        conn.commit()
        print("샘플 데이터가 추가되었습니다.")
    
    conn.close()
    print("주식 데이터 업데이트가 완료되었습니다.")

def generate_report():
    """배당금 리포트 생성"""
    conn = sqlite3.connect(DB_FILE)
    
    # 주식 보유 정보 가져오기
    holdings_df = pd.read_sql_query("SELECT * FROM stock_holdings", conn)
    
    # 배당금 정보 가져오기
    dividend_df = pd.read_sql_query("SELECT * FROM dividend_data", conn)
    
    # 데이터가 비어있는지 확인
    if holdings_df.empty:
        print("보유 주식 데이터가 없습니다.")
        conn.close()
        return None
    
    # 조인하여 배당금 소득 계산
    if not dividend_df.empty:
        df = pd.merge(holdings_df, dividend_df, on='ticker', how='inner', suffixes=('', '_div'))
        df['dividend_income'] = df['dividend_amount'] * df['shares']
        df['annual_dividend_income'] = df['annual_dividend'] * df['shares']
        
        # 월별 예상 배당금 계산 (간단한 버전)
        monthly_dividends = {}
        for _, row in df.iterrows():
            freq = row['frequency']
            annual_div = row['annual_dividend']
            shares = row['shares']
            
            if pd.isna(freq) or pd.isna(annual_div) or annual_div == 0:
                continue
                
            # 주기별 월간 배당금 계산
            if freq == 'Monthly':
                months = list(range(1, 13))
                div_per_month = annual_div / 12
            elif freq == 'Quarterly':
                months = [3, 6, 9, 12]  # 일반적인 분기 배당월
                div_per_month = annual_div / 4
            elif freq == 'Semi-Annual':
                months = [6, 12]  # 일반적인 반기 배당월
                div_per_month = annual_div / 2
            elif freq == 'Annual':
                months = [12]  # 일반적인 연간 배당월
                div_per_month = annual_div
            else:
                continue
            
            # 월별 배당금 누적
            dividend_per_payment = div_per_month * shares
            
            for month in months:
                if month not in monthly_dividends:
                    monthly_dividends[month] = 0
                monthly_dividends[month] += dividend_per_payment
        
        # 월별 배당금 데이터프레임 생성
        monthly_df = pd.DataFrame([
            {'month': month, 'month_name': datetime(2023, month, 1).strftime('%B'), 'dividend': amount}
            for month, amount in monthly_dividends.items()
        ])
        
        if not monthly_df.empty:
            monthly_df = monthly_df.sort_values('month')
    else:
        print("배당금 데이터가 없습니다. 기본 보고서만 생성합니다.")
        df = pd.DataFrame()
        monthly_df = pd.DataFrame()
    
    # 보고서 생성
    report_path = f"reports/dividend_report_{datetime.now().strftime('%Y%m%d')}.html"
    
    # HTML 리포트 생성
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"""
        <!DOCTYPE html>
        <html lang="ko">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>미국주식 배당금 분석 리포트</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                    color: #333;
                }}
                h1, h2, h3 {{
                    color: #2c3e50;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }}
                th, td {{
                    padding: 10px;
                    border: 1px solid #ddd;
                    text-align: left;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .chart-container {{
                    width: 100%;
                    height: 400px;
                    margin: 20px 0;
                }}
                .metrics-container {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .metric-card {{
                    flex: 1;
                    min-width: 200px;
                    background-color: #f8f9fa;
                    border-radius: 5px;
                    padding: 20px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }}
                .metric-title {{
                    font-size: 16px;
                    font-weight: bold;
                    margin-bottom: 10px;
                }}
                .metric-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #007bff;
                }}
            </style>
        </head>
        <body>
            <h1>미국주식 배당금 분석 리포트</h1>
            <p>생성일: {datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')}</p>
            
            <div class="metrics-container">
                <div class="metric-card">
                    <div class="metric-title">총 보유 종목 수</div>
                    <div class="metric-value">{len(holdings_df)}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-title">총 포트폴리오 가치</div>
                    <div class="metric-value">${holdings_df['total_value'].sum() if 'total_value' in holdings_df.columns else 0:.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-title">평균 수익률</div>
                    <div class="metric-value">{holdings_df['profit_loss_percent'].mean() if 'profit_loss_percent' in holdings_df.columns else 0:.2f}%</div>
                </div>
        """)
        
        # 배당금 정보가 있는 경우
        if not df.empty:
            f.write(f"""
                <div class="metric-card">
                    <div class="metric-title">총 연간 배당금</div>
                    <div class="metric-value">${df['annual_dividend_income'].sum():.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-title">평균 배당 수익률</div>
                    <div class="metric-value">{df['dividend_yield'].mean():.2f}%</div>
                </div>
            """)
        
        f.write("""
            </div>
        """)
        
        # 보유 주식 테이블
        f.write("""
            <h2>보유 주식 정보</h2>
            <table>
                <thead>
                    <tr>
                        <th>티커</th>
                        <th>회사명</th>
                        <th>보유 수량</th>
                        <th>매수가</th>
                        <th>현재가</th>
                        <th>평가 금액</th>
                        <th>수익률</th>
                    </tr>
                </thead>
                <tbody>
        """)
        
        for _, row in holdings_df.iterrows():
            current_price = row.get('current_price', 0)
            total_value = row.get('total_value', 0)
            profit_loss = row.get('profit_loss_percent', 0)
            
            profit_color = '#28a745' if profit_loss >= 0 else '#dc3545'
            
            f.write(f"""
                <tr>
                    <td>{row['ticker']}</td>
                    <td>{row['company_name']}</td>
                    <td>{row['shares']}</td>
                    <td>${row['purchase_price']:.2f}</td>
                    <td>${current_price:.2f}</td>
                    <td>${total_value:.2f}</td>
                    <td style="color: {profit_color}">{profit_loss:.2f}%</td>
                </tr>
            """)
        
        f.write("""
                </tbody>
            </table>
        """)
        
        # 월별 배당금 정보가 있는 경우
        if not monthly_df.empty:
            # Plotly 월별 배당금 차트 데이터
            months = monthly_df['month_name'].tolist()
            dividends = monthly_df['dividend'].tolist()
            cumulative = monthly_df['dividend'].cumsum().tolist()
            
            f.write("""
            <h2>월별 배당금 예측</h2>
            <div id="monthly-dividend-chart" class="chart-container"></div>
            <script>
                var trace1 = {
                    x: %s,
                    y: %s,
                    type: 'bar',
                    name: '월별 배당금',
                    marker: {color: '#4285f4'}
                };
                
                var trace2 = {
                    x: %s,
                    y: %s,
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: '누적 배당금',
                    line: {color: '#34a853', width: 3},
                    marker: {size: 8}
                };
                
                var data = [trace1, trace2];
                
                var layout = {
                    title: '월별 배당금 예측',
                    xaxis: {title: '월'},
                    yaxis: {title: '금액 ($)'},
                    legend: {x: 0, y: 1},
                    template: 'plotly_white'
                };
                
                Plotly.newPlot('monthly-dividend-chart', data, layout);
            </script>
            
            <h3>월별 배당금 상세</h3>
            <table>
                <thead>
                    <tr>
                        <th>월</th>
                        <th>예상 배당금</th>
                    </tr>
                </thead>
                <tbody>
            """ % (months, dividends, months, cumulative))
            
            for _, row in monthly_df.iterrows():
                f.write(f"""
                    <tr>
                        <td>{row['month_name']}</td>
                        <td>${row['dividend']:.2f}</td>
                    </tr>
                """)
            
            f.write("""
                </tbody>
            </table>
            """)
        
        # 배당금 지급 종목 정보가 있는 경우
        if not df.empty:
            f.write("""
            <h2>배당금 지급 종목</h2>
            <table>
                <thead>
                    <tr>
                        <th>티커</th>
                        <th>회사명</th>
                        <th>보유 수량</th>
                        <th>연간 배당금</th>
                        <th>배당 수익률</th>
                        <th>연간 배당 수입</th>
                        <th>배당 주기</th>
                    </tr>
                </thead>
                <tbody>
            """)
            
            for _, row in df.sort_values('annual_dividend_income', ascending=False).iterrows():
                f.write(f"""
                    <tr>
                        <td>{row['ticker']}</td>
                        <td>{row['company_name']}</td>
                        <td>{row['shares']}</td>
                        <td>${row['annual_dividend']:.2f}</td>
                        <td>{row['dividend_yield']:.2f}%</td>
                        <td>${row['annual_dividend_income']:.2f}</td>
                        <td>{row['frequency']}</td>
                    </tr>
                """)
            
            f.write("""
                </tbody>
            </table>
            """)
        
        # 포트폴리오 파이 차트 (종목별 비중)
        if not holdings_df.empty and 'total_value' in holdings_df.columns:
            # 데이터 준비
            pie_labels = holdings_df['ticker'].tolist()
            pie_values = holdings_df['total_value'].tolist()
            
            f.write("""
            <h2>포트폴리오 구성</h2>
            <div id="portfolio-pie-chart" class="chart-container"></div>
            <script>
                var data = [{
                    values: %s,
                    labels: %s,
                    type: 'pie',
                    hole: 0.4,
                    textinfo: 'percent+label',
                    textposition: 'inside'
                }];
                
                var layout = {
                    title: '종목별 포트폴리오 비중',
                    template: 'plotly_white'
                };
                
                Plotly.newPlot('portfolio-pie-chart', data, layout);
            </script>
            """ % (pie_values, pie_labels))
        
        # 리포트 푸터
        f.write("""
            <div style="margin-top: 30px; border-top: 1px solid #ddd; padding-top: 20px;">
                <p>이 리포트는 자동화된 로컬 시스템에 의해 생성되었습니다.</p>
                <p>투자 결정시 추가적인 조사와 전문가의 조언을 받아보세요.</p>
            </div>
        </body>
        </html>
        """)
    
    conn.close()
    print(f"배당금 리포트가 생성되었습니다: {report_path}")
    return report_path

def main():
    """메인 함수"""
    print("주식 배당금 분석기 시작...")
    
    # 데이터베이스 생성
    create_database()
    
    # CSV 파일에서 데이터 로드
    df = load_csv_data()
    
    # 주식 데이터 업데이트
    update_stock_data(df)
    
    # 리포트 생성
    report_path = generate_report()
    
    if report_path:
        print(f"프로세스가 완료되었습니다. 리포트 파일: {report_path}")
        print(f"절대 경로: {os.path.abspath(report_path)}")
        
        # 리포트 파일 자동 열기 (선택 사항)
        try:
            import webbrowser
            webbrowser.open('file://' + os.path.abspath(report_path))
            print("리포트가 웹 브라우저에서 열렸습니다.")
        except Exception as e:
            print(f"리포트를 자동으로 열지 못했습니다: {e}")
    else:
        print("리포트 생성에 실패했습니다.")

if __name__ == '__main__':
    main()