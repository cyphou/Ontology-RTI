# Contributing to Ontology to Accelerator Migration Tool

Thank you for your interest in contributing! This guide covers the development setup,
coding standards, and contribution workflow.

---

## Development Setup

### Prerequisites

- Python 3.12+
- Git

### Getting Started

```bash
# Clone the repository
git clone <repo-url>
cd OntologyAccelerator

# Create a virtual environment
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # macOS/Linux

# Install development dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ --tb=short -q
```

### Project Structure

```
src/   → Source extraction / parsing
deploy/   → Target generation
tests/        → Unit and integration tests
docs/              → Documentation
```

## Coding Standards

### Style

- Follow PEP 8
- Maximum line length: 120 characters (soft limit)
- Use type hints where practical

### Naming Conventions

- Private methods prefixed with `_`
- Constants as `UPPER_SNAKE_CASE`

### Testing

- All new features MUST have tests
- Run `pytest tests/ --tb=short -q` before committing
- Never weaken assertions to make tests pass

## Contribution Workflow

1. Create a feature branch: `git checkout -b feat/your-feature`
2. Make changes and add tests
3. Run the test suite: `pytest tests/ --tb=short -q`
4. Commit with conventional messages: `feat:`, `fix:`, `test:`, `docs:`
5. Push and create a pull request

## Multi-Agent Architecture

This project uses a specialized agent model. See `docs/AGENTS.md` for details.
Each agent has scoped ownership — check which agent owns the files you're modifying.
