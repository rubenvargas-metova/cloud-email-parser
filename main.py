import json
import os
import random
import sys
import time
import functions_framework
from googleapiclient.discovery import build
from google.auth import default
from google.auth.transport.requests import Request

# --- Configuración (DEBES ACTUALIZAR ESTOS VALORES) ---
# Reemplaza con tu ID real de Proyecto de GCP
PROJECT_ID = "course-474815" 
# El nombre del Topic de Pub/Sub que creaste
TOPIC_ID = "gmail-new-email-topic"
# Formato completo del nombre del topic
TOPIC_NAME = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"
# El correo electrónico del usuario de Gmail que deseas monitorear
GMAIL_USER_EMAIL = "ruben.vp8510@gmail.com"
# ------------------------------------


def renew_gmail_watch():
    """
    Función HTTP activada por Cloud Scheduler para renovar la solicitud users.watch.
    Requiere la Delegación a Nivel de Dominio (DWD) para autenticarse automáticamente.
    """
    
    try:
        # 1. Obtener credenciales del Service Account de la Cloud Function
        # Esto usa las Application Default Credentials (ADC)
        creds, _ = default(
            # Se requiere el scope de lectura para el watch
            scopes=['https://www.googleapis.com/auth/gmail.readonly']
        )
        
        print("Suplantar la identidad del usuario de Gmail para la solicitud (DWD)")
        # 2. Suplantar la identidad del usuario de Gmail para la solicitud (DWD)
        impersonated_creds = creds.with_subject(GMAIL_USER_EMAIL)
        
        print("Construir el servicio de la API de Gmail")
        # 3. Construir el servicio de la API de Gmail
        service = build('gmail', 'v1', credentials=impersonated_creds)
    
    except Exception as e:
        # Esto ocurre si falla la autenticación (ej. falta configurar DWD)
        return f"Error de Autenticación: {e}. Revisa la configuración de DWD.", 500

    print("Cuerpo de la solicitud de watch")
    # Cuerpo de la solicitud de watch
    watch_request_body = {
        'topicName': TOPIC_NAME,
        'labelIds': ['INBOX'], # Monitorea solo la bandeja de entrada
    }
    
    try:
        # 4. Enviar la solicitud users.watch
        # 'userId' debe ser 'me' o el correo del usuario si se usa DWD, 
        # aunque en este contexto, 'me' es suficiente si ya se suplantó la identidad.
        print("Enviar la solicitud users.watch")
        response = service.users().watch(userId=GMAIL_USER_EMAIL, body=watch_request_body).execute()
        
        return (
            f"✅ Watch renovado con éxito para {GMAIL_USER_EMAIL}. "
            f"Nuevo History ID: {response.get('historyId')}. "
            f"Expira: {response.get('expiration')}", 
            200
        )

    except Exception as e:
        print(f"Error en la solicitud Watch: {e}")
        return (
            f"❌ Fallo al renovar el watch. Revisa Pub/Sub y permisos. Error: {e}", 
            500
        )

# Start script
if __name__ == "__main__":
    try:
        print(renew_gmail_watch())
    except Exception as err:
        message = (
            f"Task #{TASK_INDEX}, " + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"
        )

        print(json.dumps({"message": message, "severity": "ERROR"}))
        # [START cloudrun_jobs_exit_process]
        sys.exit(1)  # Retry Job Task by exiting the process
        # [END cloudrun_jobs_exit_process]
# [END cloudrun_jobs_quickstart]