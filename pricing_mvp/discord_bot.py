import os, httpx, discord
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
API_URL = os.environ.get("API_URL", "http://localhost:8000")

class Client(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

client = Client()

@client.event
async def on_ready():
    await client.tree.sync()
    print(f"Bot logado como {client.user}")

@client.tree.command(name="sugerir_preco", description="Sugere um preço (não vinculante) com base no mercado.")
async def sugerir_preco(interaction: discord.Interaction, sku_key: str):
    await interaction.response.defer(thinking=True)
    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(f"{API_URL}/suggest_price", json={"sku_key": sku_key})
        data = r.json()
    msg = (
        f"**SKU:** {sku_key}\n"
        f"**Sugestão:** R$ {data['suggested_price']:.2f}\n"
        f"**Base:** P50 mercado = R$ {data['evidence']['aggregate']['comp_p50']:.2f}\n"
        f"Guardrails aplicados."
    )
    await interaction.followup.send(msg)

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Defina DISCORD_TOKEN no .env.")
    client.run(DISCORD_TOKEN)
    print("Bot iniciado.")