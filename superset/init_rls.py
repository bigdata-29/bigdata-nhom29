import os
from superset.app import create_app
from superset.extensions import db, security_manager
from superset.connectors.sqla.models import SqlaTable, RowLevelSecurityFilter

# Khởi tạo App context
app = create_app()
app.app_context().push()

def create_rls_rule(rule_name, role_name, table_name, clause_filter):
    print(f"🔒 Đang cấu hình RLS: {rule_name}...")

    # 1. Tìm Role
    role = security_manager.find_role(role_name)
    if not role:
        print(f"   ❌ Lỗi: Không tìm thấy Role '{role_name}'. Hãy tạo Role trước.")
        return

    # 2. Tìm Bảng (Table/Dataset)
    # Lưu ý: table_name phải chính xác tên bảng trong Database hoặc tên Dataset
    table = db.session.query(SqlaTable).filter_by(table_name=table_name).first()
    if not table:
        print(f"   ❌ Lỗi: Không tìm thấy bảng '{table_name}'.")
        return

    # 3. Kiểm tra xem Rule đã tồn tại chưa để tránh trùng lặp
    existing_rule = db.session.query(RowLevelSecurityFilter).filter_by(name=rule_name).first()
    
    if existing_rule:
        print(f"   ⚠️ Rule '{rule_name}' đã tồn tại. Đang cập nhật...")
        rls_filter = existing_rule
    else:
        rls_filter = RowLevelSecurityFilter()
        print(f"   ✨ Tạo mới Rule '{rule_name}'")

    # 4. Gán các thuộc tính
    rls_filter.name = rule_name
    rls_filter.clause = clause_filter # Đây là đoạn SQL WHERE
    rls_filter.filter_type = "Regular" # Hoặc "Base"
    
    # Quan hệ Many-to-Many
    if role not in rls_filter.roles:
        rls_filter.roles.append(role)
    
    if table not in rls_filter.tables:
        rls_filter.tables.append(table)

    # 5. Lưu vào DB
    try:
        db.session.add(rls_filter)
        db.session.commit()
        print(f"   ✅ Thành công: Role '{role_name}' chỉ thấy dữ liệu thỏa mãn: {clause_filter}")
    except Exception as e:
        db.session.rollback()
        print(f"   ❌ Lỗi khi lưu: {str(e)}")

# --- CẤU HÌNH CỦA BẠN TẠI ĐÂY ---
if __name__ == "__main__":
    # Kịch bản 1: Cứng (Static)
    # Role 'Finance_Team' chỉ được xem các dòng có department = 'Finance' trong bảng 'transactions'
    create_rls_rule(
        rule_name="Test_RLS_Rule",
        role_name="Test_role",
        table_name="recruitment_fact",
        clause_filter="\"Lương\" <> 'Thương lượng' AND \"Lương\" IS NOT NULL"
    )
