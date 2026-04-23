from .s03_compare_pre import ComparePreStep

class ComparePostStep(ComparePreStep):
    """Compara a estrutura FB vs PG após a migração."""

    def run(self) -> bool:
        print("--- Comparando Estrutura (Pós-Migração) ---")
        # Reutiliza a lógica de ComparePreStep
        return super().run()
