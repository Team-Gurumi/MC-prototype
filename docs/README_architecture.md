# Architecture Diagram Rendering

`docs/architecture.mmd` contains the Mermaid source for the research-paper-style architecture figure.

Mermaid CLI was not available in this environment, so `docs/architecture.png` was not generated here.

To render the PNG locally:

```bash
npx @mermaid-js/mermaid-cli -i docs/architecture.mmd -o docs/architecture.png
```

If you have Mermaid CLI installed globally:

```bash
mmdc -i docs/architecture.mmd -o docs/architecture.png
```
