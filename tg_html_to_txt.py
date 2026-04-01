"""
Telegram HTML Export → Clean TXT Converter
Конвертирует экспорт-логи Telegram (HTML) в чистый текстовый файл.

Поддерживает:
  • Групповые чаты и личные диалоги (корректное определение отправителя)
  • Режим «один файл» и «отдельные файлы»
  • Многопоточный парсинг (ProcessPoolExecutor) для ускорения на многоядерных CPU
"""

import os
import sys
import re
import glob
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("❌ Библиотека BeautifulSoup4 не установлена.")
    print("   Установите её командой: pip install beautifulsoup4")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("❌ Библиотека tqdm не установлена.")
    print("   Установите её командой: pip install tqdm")
    sys.exit(1)


# ─────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────

OUTPUT_FILE = "full_history_converted.txt"

# Количество воркеров по умолчанию: min(CPU_count, 8) —
# больше 8 редко даёт прирост на парсинге HTML
DEFAULT_WORKERS = min(multiprocessing.cpu_count(), 8)

DAYS_RU = {
    "Monday":    "Понедельник",
    "Tuesday":   "Вторник",
    "Wednesday": "Среда",
    "Thursday":  "Четверг",
    "Friday":    "Пятница",
    "Saturday":  "Суббота",
    "Sunday":    "Воскресенье",
}

MONTHS_RU = {
    "Jan": "янв", "Feb": "фев", "Mar": "мар",
    "Apr": "апр", "May": "май", "Jun": "июн",
    "Jul": "июл", "Aug": "авг", "Sep": "сен",
    "Oct": "окт", "Nov": "ноя", "Dec": "дек",
}


# ─────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────

def find_message_files(folder: str) -> list[str]:
    """Находит все файлы messages*.html в папке и сортирует их по номеру."""
    pattern = os.path.join(folder, "messages*.html")
    files = glob.glob(pattern)

    def sort_key(path):
        name = Path(path).stem           # "messages" / "messages3"
        digits = re.sub(r"\D", "", name) # "" / "3"
        return int(digits) if digits else 0

    return sorted(files, key=sort_key)


def parse_date(raw: str) -> str:
    """
    Парсит строку даты из Telegram-экспорта:
      'DD.MM.YYYY HH:MM:SS'  →  'DD.MM.YYYY HH:MM День'
      'Mon DD, YYYY HH:MM'   →  'DD.mon.YYYY HH:MM День'
    """
    raw = raw.strip()

    # Формат: DD.MM.YYYY HH:MM[:SS]
    m = re.match(r"(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2})(?::\d{2})?", raw)
    if m:
        date_str, time_str = m.group(1), m.group(2)
        try:
            dt = datetime.strptime(date_str, "%d.%m.%Y")
            return f"{date_str} {time_str} {DAYS_RU.get(dt.strftime('%A'), dt.strftime('%A'))}"
        except ValueError:
            pass

    # Формат: Mon DD, YYYY HH:MM  (английская локаль экспорта)
    m = re.match(r"([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})\s+(\d{2}:\d{2})", raw)
    if m:
        mon, day, year, time_str = m.groups()
        day = day.zfill(2)
        mon_ru = MONTHS_RU.get(mon, mon)
        try:
            dt = datetime.strptime(f"{day} {mon} {year}", "%d %b %Y")
            return f"{day}.{mon_ru}.{year} {time_str} {DAYS_RU.get(dt.strftime('%A'), dt.strftime('%A'))}"
        except ValueError:
            pass

    return raw  # fallback


def extract_text(message_div) -> str:
    """
    Извлекает чистый текст из div-сообщения,
    сохраняя переносы строк внутри сообщения с отступом.
    """
    text_div = message_div.find("div", class_="text")
    if not text_div:
        return ""

    # <br> → новая строка
    for br in text_div.find_all("br"):
        br.replace_with("\n")

    # Блочные теги добавляют перенос перед собой
    for tag in text_div.find_all(["p", "div"]):
        tag.insert_before("\n")

    lines = text_div.get_text(separator="").splitlines()

    result = []
    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            continue
        result.append(s if i == 0 else "    " + s)

    return "\n".join(result)


def is_service_message(div) -> bool:
    """True, если div — сервисное уведомление Telegram."""
    classes = div.get("class", [])
    return "service" in classes or "service_message" in classes


# ─────────────────────────────────────────────
# Ядро парсинга (запускается в отдельном процессе)
# ─────────────────────────────────────────────

def parse_file(filepath: str) -> list[dict]:
    """
    Парсит один HTML-файл Telegram-экспорта.

    Корректная логика определения отправителя для ДИАЛОГОВ и ГРУПП:
    ──────────────────────────────────────────────────────────────────
    Telegram Desktop экспортирует HTML так:

    Группа / канал:
      • Каждый первый div.message цепочки содержит div.from_name — используем его.
      • В «joined»-блоках from_name отсутствует — берём последнее известное имя.

    Личный диалог (DM):
      • Свои сообщения помечены CSS-классом «own» (иногда «self»).
        from_name либо содержит ваше имя, либо отсутствует в joined-блоках.
      • Чужие сообщения — без класса «own».
        from_name содержит имя собеседника или отсутствует в joined-блоках.

    Стратегия (два независимых «канала» last_own / last_other):
      1. Если from_name присутствует — это каноническое имя; сохраняем его
         в соответствующий канал (own или other).
      2. Если from_name отсутствует (joined) — берём имя из нужного канала.
      3. Благодаря разделению каналов, в DM «Вы» и «Собеседник» никогда
         не перепутаются местами.
    """
    messages = []

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        soup = BeautifulSoup(f, "html.parser")

    last_own:   str | None = None   # последнее имя из блоков с классом own
    last_other: str | None = None   # последнее имя из блоков без own

    for div in soup.find_all("div", class_=re.compile(r"\bmessage\b")):
        if is_service_message(div):
            continue

        classes = div.get("class", [])
        is_own  = "own" in classes or "self" in classes

        # ── Дата/время ───────────────────────────────────────────────────────
        date_tag = div.find("div", class_="pull_right date details")
        if not date_tag:
            date_tag = div.find(class_=re.compile(r"\bdate\b"))
        raw_date = ""
        if date_tag:
            raw_date = date_tag.get("title", "") or date_tag.get_text(strip=True)

        # ── Имя отправителя ──────────────────────────────────────────────────
        sender_tag = div.find("div", class_="from_name")
        if sender_tag:
            sender = sender_tag.get_text(strip=True)
            if is_own:
                last_own = sender
            else:
                last_other = sender
        else:
            # Joined-блок: определяем по каналу
            if is_own:
                sender = last_own or "Вы"
            else:
                sender = last_other or "Собеседник"

        # ── Текст ────────────────────────────────────────────────────────────
        text = extract_text(div)
        if not text:
            continue

        messages.append({
            "date":   parse_date(raw_date) if raw_date else "—",
            "sender": sender,
            "text":   text,
        })

    return messages


# ─────────────────────────────────────────────
# Форматирование
# ─────────────────────────────────────────────

def format_message(msg: dict) -> str:
    """Форматирует одно сообщение в одну (или несколько) строк."""
    lines = msg["text"].splitlines()
    first = f"[{msg['date']}] {msg['sender']}: {lines[0]}"
    return "\n".join([first] + lines[1:])


# ─────────────────────────────────────────────
# Многопроцессорный парсинг
# ─────────────────────────────────────────────

def parse_files_parallel(files: list[str], workers: int) -> dict[str, list[dict]]:
    """
    Парсит список файлов параллельно через ProcessPoolExecutor.
    Возвращает словарь {filepath: messages}.
    """
    results: dict[str, list[dict]] = {}

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_path = {executor.submit(parse_file, fp): fp for fp in files}

        with tqdm(total=len(files), desc="Парсинг файлов", unit="файл") as pbar:
            for future in as_completed(future_to_path):
                filepath = future_to_path[future]
                try:
                    results[filepath] = future.result()
                except Exception as exc:
                    tqdm.write(f"   ⚠ Ошибка в {os.path.basename(filepath)}: {exc}")
                    results[filepath] = []
                pbar.update(1)

    return results


# ─────────────────────────────────────────────
# Режимы сохранения
# ─────────────────────────────────────────────

def convert_merged(folder: str, files: list[str], workers: int) -> None:
    """Режим «один файл»: всё в full_history_converted.txt."""
    output_path = os.path.join(folder, OUTPUT_FILE)
    parsed = parse_files_parallel(files, workers)

    total = 0
    with open(output_path, "w", encoding="utf-8") as out:
        for filepath in tqdm(files, desc="Запись", unit="файл"):
            for msg in parsed.get(filepath, []):
                out.write(format_message(msg) + "\n")
                total += 1

    print(f"\n✅ Готово! Записано сообщений: {total}")
    print(f"   Файл: {os.path.abspath(output_path)}")


def convert_split(folder: str, files: list[str], workers: int) -> None:
    """Режим «отдельные файлы»: messagesNN.txt рядом с каждым HTML."""
    parsed = parse_files_parallel(files, workers)

    total = 0
    for filepath in tqdm(files, desc="Запись", unit="файл"):
        stem     = Path(filepath).stem
        digits   = re.sub(r"\D", "", stem)
        num      = int(digits) if digits else 0
        out_name = f"messages{num:02d}.txt"
        out_path = os.path.join(folder, out_name)

        messages = parsed.get(filepath, [])
        with open(out_path, "w", encoding="utf-8") as out:
            for msg in messages:
                out.write(format_message(msg) + "\n")

        total += len(messages)
        tqdm.write(f"   ✔ {out_name}  ({len(messages)} сообщ.)")

    print(f"\n✅ Готово! Записано сообщений: {total}")
    print(f"   Файлы сохранены в: {os.path.abspath(folder)}")


# ─────────────────────────────────────────────
# Интерфейс (вопросы к пользователю)
# ─────────────────────────────────────────────

def ask_save_mode() -> str:
    print("\nКак сохранить результат?")
    print("  [1] Один файл       — full_history_converted.txt")
    print("  [2] Отдельные файлы — messages01.txt, messages02.txt, ...")
    while True:
        choice = input("Ваш выбор (1/2): ").strip()
        if choice in ("1", "2"):
            return choice
        print("   Введите 1 или 2.")


def ask_workers() -> int:
    cpu       = multiprocessing.cpu_count()
    suggested = min(cpu, 8)
    print(f"\nСколько процессов использовать для парсинга?")
    print(f"  [Enter] Авто ({suggested} из {cpu} доступных ядер)")
    print(f"  Или введите число от 1 до {cpu}")
    while True:
        val = input(f"Кол-во процессов [{suggested}]: ").strip()
        if val == "":
            return suggested
        if val.isdigit() and 1 <= int(val) <= cpu:
            return int(val)
        print(f"   Введите число от 1 до {cpu} или нажмите Enter.")


# ─────────────────────────────────────────────
# Точка входа
# ─────────────────────────────────────────────

def main() -> None:
    # ── 1. Определяем папку ──────────────────────────────────────────────────
    script_dir = Path(sys.argv[0]).resolve().parent

    if len(sys.argv) >= 2:
        folder = sys.argv[1]
    else:
        candidate_files = find_message_files(str(script_dir))
        if candidate_files:
            folder = str(script_dir)
            print(f"📂 HTML-файлы найдены рядом со скриптом: {folder}")
        else:
            print("📂 Файлы messages*.html не найдены рядом со скриптом.")
            folder = input("📁 Укажите путь к папке с экспортом Telegram: ").strip().strip('"')

    if not os.path.isdir(folder):
        print(f"❌ Папка не найдена: {folder}")
        sys.exit(1)

    # ── 2. Ищем файлы ────────────────────────────────────────────────────────
    files = find_message_files(folder)
    if not files:
        print(f"❌ Файлы messages*.html не найдены в папке: {folder}")
        sys.exit(1)

    print(f"\n✅ Найдено файлов: {len(files)}")
    for f in files:
        print(f"   • {os.path.basename(f)}")

    # ── 3. Параметры конвертации ─────────────────────────────────────────────
    mode    = ask_save_mode()
    workers = ask_workers()

    print(f"\n🔄 Запускаем конвертацию ({workers} процессов)...\n")

    # ── 4. Конвертация ───────────────────────────────────────────────────────
    try:
        if mode == "1":
            convert_merged(folder, files, workers)
        else:
            convert_split(folder, files, workers)
    except KeyboardInterrupt:
        print("\n\n⛔ Конвертация прервана.")
        sys.exit(0)


# Guard обязателен для корректной работы ProcessPoolExecutor на Windows
if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
