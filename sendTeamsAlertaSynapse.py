%py
import requests
import json

def send_message_to_teams():
    teams_webhook_url = "link gerado a partir do teams - Equipes > ... > Conectores > Incoming Webhook"

    headers = {
        "Content-Type": "application/json"
    }

    message_data = { 
            {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "0078D7",
                "summary": "Azure Synapse Error",
                "sections": [
                    {
                        "activityTitle": "**~Alertinha~**",
                        "activitySubtitle": "~*Assistente de Erro das ferramentas Azure Cloud*~",
                        "activityImage": "https://img.youtube.com/vi/BBGYQEjdvxw/0.jpg",
                        "text": "**_@{pipeline().Pipeline}_**",
                        "facts": [
                            {
                                "name": "Erro:",
                                "value": "@{activity('Execute Pipeline1').Error.errorCode}" 
                            },
                            {
                                "name": "Descrição do Erro:",
                                "value": "@{activity('Execute Pipeline1').Error.message}"
                            },
                            {
                                "name": "runId",
                                "value": "<a href='https://web.azuresynapse.net/pt-br/monitoring/pipelineruns/@{pipeline().RunId}?workspace=fsfdfsdfFresourceGroups%2FrgdasdfsdfsdfsdfsdfGMicrosoft.Synapse%2Hworkspaces%2issdftfgdfgwprdfgeu2'>@{pipeline().RunId}</a>"
                            }, # de acordo com a URL para que possa abrir sendo adicionado dinamicamente apenas o runId
                            
                        ]
                    },
                    {
                        "activityTitle": "Synapse",
                        "activitySubtitle": "*Horário de disparo do erro*",
                        "activityImage": "https://seeklogo.com/images/A/azure-synapse-analytics-logo-B87A556A9C-seeklogo.com.png",
                        "text": "Horário de Brasília: @{formatDateTime(addhours(utcNow(),-3), 'dd/MM/yyyy HH:mm')}"
                    }
                ]
            }                
            }

    try:
        response = requests.post(teams_webhook_url, headers=headers, data=json.dumps(message_data))
        response.raise_for_status()
        print("Message sent successfully to Microsoft Teams.")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message to Microsoft Teams: {e}")

if __name__ == "__main__":
    send_message_to_teams()
