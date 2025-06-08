from airflow.www.app import create_app
from flask_appbuilder.security.sqla.models import User
from flask_appbuilder.security.manager import SecurityManager

app = create_app()

with app.app_context():
    sm: SecurityManager = app.appbuilder.sm

    if sm.find_user(username='eddi'):
        print("⚠️ 사용자 'eddi'가 이미 존재합니다.")
    else:
        user = sm.add_user(
            username='eddi',
            first_name='Sanghoon',
            last_name='Lee',
            email='eddi@example.com',
            role=sm.find_role('Admin'),
            password='eddi@123'
        )
        print(f"✅ 사용자 생성 완료: {user}")
