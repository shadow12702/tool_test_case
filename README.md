## Chat Completions Export (Flask)

### Tính năng
- **Cấu hình API** trong `config/api_config.json`
- **Đọc input từ Excel/CSV**:
  - XLSX: đọc **tất cả sheet**
  - Cột **`Prompt`** (hoặc `user_input`) được map thành `user_input`
  - Hỗ trợ file **XLSX nhưng đặt tên `.csv`** (detect magic bytes `PK`)
- **Chạy song song theo từng user (multi-thread)** (tối đa **8 thread**, cấu hình trong `config/api_config.json`)
- **Export** theo thư mục: `export/<user_name>/<run_id>/...`

### Chạy batch (không cần UI)
Bạn đang có `.venv/` sẵn trong workspace, nên ưu tiên dùng Python trong venv.

```bash
.\.venv\Scripts\python run_batch.py --users csv/listUser.csv --prompts csv/prompt_txt_to_sql.xlsx
```

Tuỳ chọn override body:

```bash
.\.venv\Scripts\python run_batch.py --overrides "{\"model_name\":\"gemma3:12b-it-qat\",\"temperature\":0.6}"
```

### Output
- Mỗi user sẽ có thư mục riêng: `export/<user_name>/<run_id>/`
- Tổng hợp batch: `export/<run_id>_summary.json`

