@echo off
echo ========================================
echo ARGO Ocean Intelligence System
echo ========================================
echo.
echo Starting backend server...
:: Navigates to venv, activates it, and runs uvicorn
start "Backend" cmd /k ".\venv\Scripts\activate && python -m uvicorn backend16:app --reload"

timeout /t 60 /nobreak > nul
echo.
echo Starting frontend...
:: Navigates to venv, activates it, and runs streamlit
start "Frontend" cmd /k ".\venv\Scripts\activate && streamlit run app.py"

echo.
echo ========================================
echo System started!
echo Backend: http://127.0.0.1:8000
echo Frontend: http://localhost:8501
echo ========================================
