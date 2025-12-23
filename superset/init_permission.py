import os
# cài đặt superert
from superset.app import create_app
from superset.extensions import security_manager

# Khởi tạo App context
app = create_app()
app.app_context().push()

def create_custom_role(role_name, table_names):
    print(f"🚀 Đang tạo Role: {role_name}...")
    
    # 1. Tạo hoặc lấy Role
    role = security_manager.find_role(role_name)
    if not role:
        role = security_manager.add_role(role_name)
        print(f"   - Đã tạo mới Role '{role_name}'")
    
    # 2. Các quyền cơ bản để xem được Dashboard (Bắt buộc phải có)
    base_perms = [
        ("can_list", "Dashboard"),
        ("can_show", "Dashboard"),
        ("can_list", "Slice"), # Slice là Chart
        ("can_show", "Slice"),
        ("can_explore", "Superset"), # Để xem chart mode explore
    ]
    
    for action, resource in base_perms:
        pvm = security_manager.find_permission_view_menu(action, resource)
        if pvm:
            security_manager.add_permission_role(role, pvm)

    # 3. Gán quyền truy cập Dataset (Table) cụ thể
    # Format quyền trong DB thường là: [database_name].[table_name](id:...)
    # Cách an toàn nhất là tìm permission view menu có tên chứa tên bảng
    
    all_pvm = security_manager.get_all_view_menu_access("datasource_access")
    
    for table in table_names:
        # Lọc ra quyền access đúng bảng mình cần
        # Lưu ý: Tên datasource trong Superset thường kèm cả tên Database
        target_pvm = [p for p in all_pvm if table in p.view_menu.name]
        
        if target_pvm:
            for pvm in target_pvm:
                security_manager.add_permission_role(role, pvm)
                print(f"   - Đã gán quyền truy cập bảng: {pvm.view_menu.name}")
        else:
            print(f"   ⚠️ Không tìm thấy bảng '{table}'. Hãy chắc chắn bạn đã add Dataset này trên UI rồi.")

    print(f"✅ Hoàn tất Role: {role_name}")

# --- CẤU HÌNH CỦA BẠN TẠI ĐÂY ---
if __name__ == "__main__":

    create_custom_role("test_permission", ["recruitment_fact"])

    