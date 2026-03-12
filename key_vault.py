from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def get_secret_from_keyvault(vault_url: str, secret_name: str) -> str:
    """
    Retrieve a secret value from Azure Key Vault.
    Args:
        vault_url (str): The URL of the Azure Key Vault (e.g., 'https://<your-key-vault-name>.vault.azure.net/')
        secret_name (str): The name of the secret to retrieve
    Returns:
        str: The value of the secret
    """
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

if __name__ == "__main__":
    # Example usage
    vault_url = "https://kv-myvalur.vault.azure.net/"  # Replace with your Key Vault URL
    secret_name = "kv-myvalur-pwd"  # Replace with your secret name
    secret_value = get_secret_from_keyvault(vault_url, secret_name)
    print(f"Secret value: {secret_value}")
