# Changelog

Alle væsentlige ændringer til projektet dokumenteres i denne fil.

---

## [2026-02-17] - Multi-EM Forbedringer og Bugfixes

### 🐛 Rettet
#### Multi-Client Tilføjelse Fejl (server_app.py)
- **Problem:** Når man tilføjede 3 EM'er i samme EM gruppe, blev kun 1 tilføjet. De andre 2 fejlede med "group_data_collection mismatch" fejl.
- **Årsag:** `group_data_collection` blev genereret baseret på **individuelle device navne** (EM-1, EM-2, EM-3), hvilket gav forskellige collection navne for hver EM.
- **Løsning:** Ændrede logikken til at bruge **group_device_name** (det fælles gruppenavn) i stedet.
  - **Før:** `10340_EM-1`, `10340_EM-2`, `10340_EM-3` (forskellige ❌)
  - **Efter:** `10340_EM_1`, `10340_EM_1`, `10340_EM_1` (samme ✅)
- **Filer:** 
  - `EMServer_2026/server_app.py` (linje ~500-520)
  - `EMServer_2026/server_app.py` (linje ~640-660 i update_device)

#### Register Validering (clients_v3.py)
- **Problem:** Systemet tjekker ikke korrekt om import/export registre er tomme eller None.
- **Før:** `has_imp = "total_import" in regs` - tjekker kun om nøglen eksisterer
- **Efter:** Validerer også at:
  - Værdien ikke er `None`, `""`, eller `{}`
  - Det er en dict
  - Den har en gyldig `address` værdi
- **Effekt:** Systemet beregner nu korrekt energi fra power når registre mangler eller er ugyldige
- **Filer:** 
  - `clients_v3.py` (linje ~465-467 for single devices)
  - `clients_v3.py` (linje ~1069-1081 for EM groups)

---

### ✨ Nye Features

#### Multi-EM Gruppe Redigering (clients.html)
- **Feature:** Mulighed for at redigere hele multi-EM grupper på én gang
- **Funktionalitet:**
  - Klik "Rediger Gruppe" på EM group header
  - Multi-EM modal åbner med alle eksisterende data pre-udfyldt
  - Rediger registre, IP'er, porte, formats, scales osv.
  - Gem ændringer - opdaterer alle medlemmer samtidig
- **Implementering:**
  - Ny funktion: `editMultiEmGroup(button)`
  - Understøtter både `/add_device` og `/update_device` endpoints
  - Bruger `device_id` til at identificere specifikke devices ved opdatering
- **Filer:** `EMServer_2026/templates/clients.html`

#### Gruppe Beskyttelse i Edit Mode (clients.html)
- **Feature:** Beskyttelse mod at splitte EM grupper ved redigering
- **Funktionalitet:**
  - Når du redigerer en EM gruppe, låses følgende felter:
    - ✅ EM Group Navn (readonly)
    - ✅ Server Unit ID (readonly)
    - ✅ Group Device Name (disabled)
  - Grå baggrund og "not-allowed" cursor indikerer låste felter
  - Hint tekst opdateres til: "🔒 Låst - kan ikke ændres i edit mode"
  - Gul advarselsboks vises i toppen af modal
- **Hvad du stadig kan redigere:**
  - Register adresser (active power, import, export)
  - IP adresser og porte
  - Unit IDs (Modbus ID'er)
  - Formats, scales, offsets
  - Read types (holding/input)
  - Site navne og device navne
  - Project nummer
- **Implementering:**
  - `isEditingMultiEm` flag tracker edit/add mode
  - Automatisk unlock når modal lukkes
  - Edit mode warning div (`#editModeWarning`)
- **Filer:** `EMServer_2026/templates/clients.html`

---

### 🔧 Forbedringer

#### Konsistent group_data_collection Generering
- Både `add_device` og `update_device` endpoints bruger nu samme logik
- Understøtter fallback hvis `group_device_name` mangler:
  1. `project_nr_group_device_name` (foretrukket)
  2. `group_device_name` (uden project nr)
  3. `project_nr_em_group` (fallback)
  4. `em_group` (sidste fallback)
- Auto-generering logger altid hvilket navn der blev brugt

#### UI/UX Forbedringer
- Knappen på EM group headers hedder nu "Rediger Gruppe" i stedet for bare "Rediger"
- Modal titel ændres dynamisk: "🔌 Tilføj Multi-EM Klient" vs "✏️ Rediger Multi-EM Gruppe"
- Submit knap tekst ændres: "Tilføj Alle EM'er" vs "Gem Ændringer"
- Success/error beskeder skelner mellem "tilføjet" og "opdateret"

---

### 📚 Dokumentation

#### Energy Beregning fra Power
- Dokumenteret hvordan systemet håndterer EM'er uden import/export registre
- Systemet beregner automatisk energi ved integration: `energy_wh = (power × interval) / 3600`
- Fungerer både for single devices og multi-EM groups
- Metadata markeres med: "Beregnet fra active power"

---

### 🔍 Tekniske Detaljer

#### Validerings-logik
```python
# Single devices
has_imp = "total_import" in regs and \
          regs.get("total_import") and \
          isinstance(regs.get("total_import"), dict) and \
          regs["total_import"].get("address")

# Multi-EM groups
def has_valid_register(member_cfg: dict, reg_name: str) -> bool:
    regs = (member_cfg.get("registers", {}) or {})
    reg = regs.get(reg_name)
    return bool(reg and isinstance(reg, dict) and reg.get("address"))
```

#### Felt-låsning (JavaScript)
```javascript
// Lock
element.setAttribute('readonly', 'true');
element.style.backgroundColor = '#f0f0f0';
element.style.cursor = 'not-allowed';

// For select: bruges pointer-events i stedet for disabled
selectElement.style.pointerEvents = 'none';
```

---

### 📝 Bemærkninger

- Alle ændringer er bagudkompatible
- Eksisterende data påvirkes ikke
- Frontend changes kræver kun browser refresh
- Backend changes kræver server restart

---

### 🧪 Test Anbefalinger

1. **Multi-EM Tilføjelse:**
   - Tilføj 3+ EM'er i samme gruppe
   - Verificer at alle tilføjes (ikke kun 1)
   - Tjek at `group_data_collection` er identisk for alle

2. **Multi-EM Redigering:**
   - Klik "Rediger Gruppe" på en eksisterende EM gruppe
   - Verificer at felter er låste (EM Group, Server Unit ID, Group Device Name)
   - Rediger register adresser og gem
   - Verificer at gruppen forbliver intakt

3. **Register Validering:**
   - Opret EM med kun active_power (ingen import/export)
   - Verificer at energy beregnes automatisk
   - Tjek MongoDB for "Beregnet fra active power" i metadata

4. **Edge Cases:**
   - Tomme register felter (`""`, `None`, `{}`)
   - Registre med tom address: `{"address": ""}`
   - Mixed scenarios: nogle EM'er med registre, andre uden

---

### 👥 Contributors
- GitHub Copilot (AI Assistant)

### 🔗 Relaterede Issues
- Multi-client tilføjelse viser "1 ud 3 EM tilføjet"
- Kan ikke redigere multi-EM register adresser
- Gruppe bliver splittet ved redigering
- Register validering håndterer ikke None/tomme værdier
