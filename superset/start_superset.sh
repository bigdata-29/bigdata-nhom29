#!/bin/bash

echo "🚀 Đang khởi động hệ thống làm việc..."

# 1. Kiểm tra và bật Minikube nếu chưa chạy
if ! minikube status | grep -q "Running"; then
    echo "📦 Minikube chưa bật. Đang khởi động Minikube..."
    minikube start
else
    echo "✅ Minikube đang chạy."
fi

# 2. Bật Superset Service (Cần nhập mật khẩu sudo 1 lần)
echo "🌐 Đang bật Superset Service..."
sudo systemctl start superset
echo "✅ Superset Service đã được bật."

# 3. Chạy Port-Forward cho Database
echo "🔗 Đang kết nối tới Database (Port-Forwarding)..."
echo "⚠️  VUI LÒNG KHÔNG TẮT CỬA SỔ NÀY KHI ĐANG LÀM VIỆC ⚠️"
echo "👉 Truy cập Superset tại: http://localhost:8088"
echo "👉 Nhấn [Ctrl + C] để dừng làm việc và ngắt kết nối."

# Lệnh này sẽ treo ở đây để giữ kết nối
kubectl port-forward svc/postgres-service -n postgres 5432:5432