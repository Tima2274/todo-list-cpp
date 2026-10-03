"""
Генератор тестовых веб-логов для практики 1.7 — Spark.
Создаёт реалистичный лог-файл в формате Common Log Format.

Используется для задания 2 (анализ веб-логов) и задания 3 (расширенный анализ).

Usage:
    python generate_web_logs.py
    python generate_web_logs.py --output datasets/web_logs.txt --num 2000
"""

import random
import argparse
from pathlib import Path


def generate_web_logs(output_path: str = "datasets/web_logs.txt",
                      num_entries: int = 1000) -> dict:
    """
    Генерирует тестовые веб-логи.
    
    Args:
        output_path: Путь к выходному файлу
        num_entries: Количество записей в логе
    
    Returns:
        Словарь со статистикой сгенерированного лога
    """
    # Реалистичные данные
    ips = [
        "192.168.1.10", "192.168.1.25", "192.168.1.50", "192.168.1.100",
        "10.0.0.15", "10.0.0.30", "10.0.0.45", "10.0.0.60",
        "172.16.0.5", "172.16.0.10", "172.16.0.20", "172.16.0.30",
        "203.0.113.50", "203.0.113.100", "198.51.100.25", "198.51.100.75",
        "192.0.2.100", "192.0.2.200", "192.0.2.50", "192.0.2.150",
    ]
    
    # Распределение IP — некоторые чаще других (имитация ботов/сканеров)
    ip_weights = [15, 12, 10, 8, 7, 6, 5, 5, 5, 5, 4, 4, 3, 3, 2, 2, 2, 2, 1, 1]
    
    methods = ["GET", "GET", "GET", "GET", "POST", "PUT", "DELETE", "HEAD"]
    method_weights = [60, 15, 10, 5, 5, 3, 1, 1]
    
    paths = [
        ("/", 15), ("/index.html", 12), ("/about", 8), ("/contact", 5),
        ("/api/users", 10), ("/api/data", 10), ("/api/login", 8),
        ("/images/logo.png", 7), ("/css/style.css", 6), ("/js/app.js", 6),
        ("/docs/readme", 4), ("/dashboard", 5), ("/settings", 3),
        ("/api/reports", 4), ("/health", 5), ("/favicon.ico", 4),
        ("/robots.txt", 2), ("/sitemap.xml", 2), ("/api/search", 6),
        ("/blog", 3), ("/blog/post/1", 2), ("/blog/post/2", 2),
    ]
    path_list = [p[0] for p in paths]
    path_weights = [p[1] for p in paths]
    
    statuses = [200, 200, 200, 200, 200, 200, 301, 302, 304, 400, 401, 403, 404, 404, 500, 502, 503]
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)",
        "curl/7.88.1",
        "python-requests/2.28.0",
        "Googlebot/2.1 (+http://www.google.com/bot.html)",
    ]
    
    # Генерация дат
    days = range(1, 29)
    hours = range(0, 24)
    minutes = range(0, 60)
    seconds = range(0, 60)
    
    logs = []
    status_counts = {}
    ip_counts = {}
    generated_paths = set()
    
    random.seed(42)  # Детерминированная генерация
    
    for _ in range(num_entries):
        ip = random.choices(ips, weights=ip_weights, k=1)[0]
        method = random.choices(methods, weights=method_weights, k=1)[0]
        path = random.choices(path_list, weights=path_weights, k=1)[0]
        status = random.choice(statuses)
        size = random.randint(200, 50000) if status == 200 else random.randint(0, 500)
        day = random.choice(days)
        hour = random.choice(hours)
        minute = random.choice(minutes)
        second = random.choice(seconds)
        date = f"{day:02d}/Oct/2024:{hour:02d}:{minute:02d}:{second:02d}"
        ua = random.choice(user_agents)
        
        log_line = f'{ip} - - [{date}] "{method} {path} HTTP/1.1" {status} {size} "{ua}"'
        logs.append(log_line)
        
        # Статистика
        status_counts[status] = status_counts.get(status, 0) + 1
        ip_counts[ip] = ip_counts.get(ip, 0) + 1
        generated_paths.add(path)
    
    # Сохранение
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(logs) + "\n", encoding="utf-8")
    
    # Статистика
    stats = {
        "num_entries": num_entries,
        "unique_ips": len(ip_counts),
        "unique_paths": len(generated_paths),
        "status_distribution": status_counts,
        "top_ips": sorted(ip_counts.items(), key=lambda x: x[1], reverse=True)[:10],
        "file_size_bytes": output.stat().st_size,
    }
    
    print(f"Web logs generated: {num_entries} entries")
    print(f"Unique IPs: {stats['unique_ips']}")
    print(f"File size: {stats['file_size_bytes']} bytes")
    print(f"Status distribution: {stats['status_distribution']}")
    print(f"Saved to: {output}")
    
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate web logs for practice 1.7")
    parser.add_argument("--output", "-o", type=str, default="datasets/web_logs.txt",
                        help="Output file path")
    parser.add_argument("--num", "-n", type=int, default=1000,
                        help="Number of log entries to generate")
    
    args = parser.parse_args()
    
    stats = generate_web_logs(output_path=args.output, num_entries=args.num)
    print(f"\nTop 5 IPs:")
    for ip, count in stats["top_ips"][:5]:
        print(f"  {ip}: {count} requests")
