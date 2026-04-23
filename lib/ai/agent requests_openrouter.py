import os
import json
from typing import Optional, List
from anthropic import Anthropic
from .prompts import SYSTEM_PROMPT, SCHEMA_DIFF_PROMPT, ERROR_DIAGNOSIS_PROMPT

class MigrationAI:
    """Interface com o Claude da Anthropic para assistência em erros e diferenças de schema."""

    def __init__(self, api_key: Optional[str] = None, model: str = "claude-3-7-sonnet-latest"):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self.model = model
        self.client = None
        if self.api_key:
            self.client = Anthropic(api_key=self.api_key)

    def is_available(self) -> bool:
        return self.client is not None

    def suggest_schema_fix(self, diff_context: str) -> str:
        """Sugere scripts SQL de correção para diferenças de schema."""
        if not self.is_available():
            return "-- AI Not Configured --"
        
        prompt = SCHEMA_DIFF_PROMPT.format(diff_context=diff_context)
        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}]
            )
            # Extração básica de SQL se estiver entre blocos ```sql
            text = message.content[0].text
            if "```sql" in text:
                return text.split("```sql")[1].split("```")[0].strip()
            return text.strip()
        except Exception as e:
            return f"-- AI Error: {str(e)} --"

    def diagnose_error(self, step_name: str, table_name: Optional[str], 
                       error_message: str, context: dict = None) -> str:
        """Analisa um erro de execução e sugere causas/correções."""
        if not self.is_available():
            return "AI Not Configured"

        prompt = ERROR_DIAGNOSIS_PROMPT.format(
            step_name=step_name,
            table_name=table_name or "N/A",
            error_message=error_message,
            context_json=json.dumps(context or {}, indent=2)
        )
        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text.strip()
        except Exception as e:
            return f"AI Error: {str(e)}"
