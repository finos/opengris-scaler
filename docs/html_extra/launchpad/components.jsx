// Shared components for openGRIS Scaler Launchpad
// Exports to window.SC

const { useState, useEffect, useRef, useCallback } = React;

const OCI_SHAPE_PRICING = {
  "CI.Standard.A1.Flex": { ocpuPrice: 0.013106, memPrice: 0.0019659 },
  "CI.Standard.E4.Flex": { ocpuPrice: 0.032765, memPrice: 0.0019659 },
};

/* ── SecretInput ── */
function SecretInput({ value, onChange, placeholder, style }) {
  const [visible, setVisible] = useState(false);
  return (
    <div
      style={{
        position: "relative",
        display: "flex",
        alignItems: "center",
        ...style,
      }}
    >
      <input
        type={visible ? "text" : "password"}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        spellCheck={false}
        autoComplete="off"
        style={{
          flex: 1,
          background: "transparent",
          border: "none",
          outline: "none",
          color: "inherit",
          font: "inherit",
          padding: 0,
          paddingRight: "36px",
        }}
      />
      <button
        onClick={() => setVisible((v) => !v)}
        title={visible ? "Hide" : "Show"}
        style={{
          position: "absolute",
          right: 0,
          background: "none",
          border: "none",
          cursor: "pointer",
          padding: "0 0 0 8px",
          color: visible ? "var(--text-accent)" : "var(--text-muted)",
          fontSize: "11px",
          fontFamily: "inherit",
          letterSpacing: "normal",
        }}
      >
        {visible ? "Hide" : "Show"}
      </button>
    </div>
  );
}

/* ── RegionSelect ── */
function RegionSelect({ value, onChange }) {
  const regions = window.SCALER_REGIONS || [];
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const triggerRef = useRef(null);
  const dropdownRef = useRef(null);
  const [dropdownStyle, setDropdownStyle] = useState({});

  const filtered = regions.filter(
    (r) =>
      r.value.toLowerCase().includes(search.toLowerCase()) ||
      r.label.toLowerCase().includes(search.toLowerCase()),
  );

  useEffect(() => {
    function handleClick(e) {
      if (
        triggerRef.current &&
        !triggerRef.current.contains(e.target) &&
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target)
      ) {
        setOpen(false);
        setSearch("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const openDropdown = () => {
    if (!triggerRef.current) return;
    const r = triggerRef.current.getBoundingClientRect();
    setDropdownStyle({
      position: "fixed",
      top: r.bottom + 4,
      left: r.left,
      width: r.width,
    });
    setSearch("");
    setOpen(true);
  };

  const selected = regions.find((r) => r.value === value);

  return (
    <div ref={triggerRef} style={{ position: "relative" }}>
      <button
        onClick={() => (open ? setOpen(false) : openDropdown())}
        style={{
          width: "100%",
          background: "var(--bg-surface)",
          border: "1px solid var(--border-accent)",
          borderRadius: 3,
          padding: "8px 10px",
          color: "var(--text-primary)",
          fontFamily: "inherit",
          fontSize: 12,
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
          textAlign: "left",
          outline: "none",
        }}
      >
        <span style={{ flex: 1 }}>
          <span style={{ color: "var(--text-secondary)", fontWeight: 600 }}>
            {value}
          </span>
          {selected && (
            <span
              style={{
                color: "var(--text-muted)",
                marginLeft: 10,
                fontSize: 11,
              }}
            >
              {selected.label}
            </span>
          )}
        </span>
        <span
          style={{
            display: "inline-block",
            width: 7,
            height: 7,
            borderRight: "1.5px solid var(--text-muted)",
            borderBottom: "1.5px solid var(--text-muted)",
            transform: open ? "rotate(225deg)" : "rotate(45deg)",
            position: "relative",
            top: open ? "2px" : "-2px",
            flexShrink: 0,
          }}
        />
      </button>
      {open &&
        ReactDOM.createPortal(
          <div
            ref={dropdownRef}
            style={{
              ...dropdownStyle,
              background: "var(--bg-elevated)",
              border: "1px solid var(--border-strong)",
              borderRadius: 4,
              zIndex: 9999,
              boxShadow: "0 16px 48px rgba(0,0,0,0.7)",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                padding: "8px 10px",
                borderBottom: "1px solid rgba(255,255,255,0.06)",
              }}
            >
              <input
                autoFocus
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search regions…"
                style={{
                  width: "100%",
                  background: "var(--bg-surface)",
                  border: "1px solid rgba(255,255,255,0.1)",
                  borderRadius: 3,
                  padding: "6px 9px",
                  color: "var(--text-primary)",
                  fontFamily: "inherit",
                  fontSize: 12,
                  outline: "none",
                }}
              />
            </div>
            <div style={{ maxHeight: 260, overflowY: "auto" }}>
              {filtered.map((r) => (
                <div
                  key={r.value}
                  onClick={() => {
                    onChange(r.value);
                    setOpen(false);
                    setSearch("");
                  }}
                  style={{
                    padding: "8px 12px",
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "baseline",
                    gap: 10,
                    background:
                      r.value === value
                        ? "rgba(0,200,224,0.08)"
                        : "transparent",
                    borderBottom: "1px solid rgba(255,255,255,0.03)",
                  }}
                  onMouseEnter={(e) => {
                    if (r.value !== value)
                      e.currentTarget.style.background = "var(--bg-surface)";
                  }}
                  onMouseLeave={(e) => {
                    if (r.value !== value)
                      e.currentTarget.style.background = "transparent";
                  }}
                >
                  <span
                    style={{
                      fontSize: 12,
                      fontWeight: 600,
                      color:
                        r.value === value
                          ? "var(--text-success)"
                          : "var(--text-primary)",
                      flexShrink: 0,
                    }}
                  >
                    {r.value}
                  </span>
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                    {r.label}
                  </span>
                </div>
              ))}
              {filtered.length === 0 && (
                <div
                  style={{
                    padding: 20,
                    textAlign: "center",
                    color: "var(--text-dim)",
                    fontSize: 12,
                  }}
                >
                  No regions match
                </div>
              )}
            </div>
          </div>,
          document.body,
        )}
    </div>
  );
}

/* ── OciRegionSelect ── */
function OciRegionSelect({ value, onChange }) {
  const regions = window.SCALER_OCI_REGIONS || [];
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const triggerRef = useRef(null);
  const dropdownRef = useRef(null);
  const [dropdownStyle, setDropdownStyle] = useState({});

  const filtered = regions.filter(
    (r) =>
      r.value.toLowerCase().includes(search.toLowerCase()) ||
      r.label.toLowerCase().includes(search.toLowerCase()),
  );

  useEffect(() => {
    function handleClick(e) {
      if (
        triggerRef.current &&
        !triggerRef.current.contains(e.target) &&
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target)
      ) {
        setOpen(false);
        setSearch("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const openDropdown = () => {
    if (!triggerRef.current) return;
    const r = triggerRef.current.getBoundingClientRect();
    setDropdownStyle({
      position: "fixed",
      top: r.bottom + 4,
      left: r.left,
      width: r.width,
    });
    setSearch("");
    setOpen(true);
  };

  const selected = regions.find((r) => r.value === value);

  return (
    <div ref={triggerRef} style={{ position: "relative" }}>
      <button
        onClick={() => (open ? setOpen(false) : openDropdown())}
        style={{
          width: "100%",
          background: "var(--bg-surface)",
          border: "1px solid var(--border-accent)",
          borderRadius: 3,
          padding: "8px 10px",
          color: "var(--text-primary)",
          fontFamily: "inherit",
          fontSize: 12,
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
          textAlign: "left",
          outline: "none",
        }}
      >
        <span style={{ flex: 1 }}>
          {value ? (
            <>
              <span style={{ color: "var(--text-secondary)", fontWeight: 600 }}>
                {value}
              </span>
              {selected && (
                <span style={{ color: "var(--text-muted)", marginLeft: 10, fontSize: 11 }}>
                  {selected.label}
                </span>
              )}
            </>
          ) : (
            <span style={{ color: "var(--text-dim)" }}>Select region…</span>
          )}
        </span>
        <span
          style={{
            display: "inline-block",
            width: 7,
            height: 7,
            borderRight: "1.5px solid var(--text-muted)",
            borderBottom: "1.5px solid var(--text-muted)",
            transform: open ? "rotate(225deg)" : "rotate(45deg)",
            position: "relative",
            top: open ? "2px" : "-2px",
            flexShrink: 0,
          }}
        />
      </button>
      {open &&
        ReactDOM.createPortal(
          <div
            ref={dropdownRef}
            style={{
              ...dropdownStyle,
              background: "var(--bg-elevated)",
              border: "1px solid var(--border-strong)",
              borderRadius: 4,
              zIndex: 9999,
              boxShadow: "0 16px 48px rgba(0,0,0,0.7)",
              overflow: "hidden",
            }}
          >
            <div style={{ padding: "8px 10px", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
              <input
                autoFocus
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search regions…"
                style={{
                  width: "100%",
                  background: "var(--bg-surface)",
                  border: "1px solid rgba(255,255,255,0.1)",
                  borderRadius: 3,
                  padding: "6px 9px",
                  color: "var(--text-primary)",
                  fontFamily: "inherit",
                  fontSize: 12,
                  outline: "none",
                }}
              />
            </div>
            <div style={{ maxHeight: 260, overflowY: "auto" }}>
              {filtered.map((r) => (
                <div
                  key={r.value}
                  onClick={() => { onChange(r.value); setOpen(false); setSearch(""); }}
                  style={{
                    padding: "8px 12px",
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "baseline",
                    gap: 10,
                    background: r.value === value ? "rgba(0,200,224,0.08)" : "transparent",
                    borderBottom: "1px solid rgba(255,255,255,0.03)",
                  }}
                  onMouseEnter={(e) => { if (r.value !== value) e.currentTarget.style.background = "var(--bg-surface)"; }}
                  onMouseLeave={(e) => { if (r.value !== value) e.currentTarget.style.background = "transparent"; }}
                >
                  <span style={{ fontSize: 12, fontWeight: 600, color: r.value === value ? "var(--text-success)" : "var(--text-primary)", flexShrink: 0 }}>
                    {r.value}
                  </span>
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>{r.label}</span>
                </div>
              ))}
              {filtered.length === 0 && (
                <div style={{ padding: 20, textAlign: "center", color: "var(--text-dim)", fontSize: 12 }}>
                  No regions match
                </div>
              )}
            </div>
          </div>,
          document.body,
        )}
    </div>
  );
}

/* ── OciShapeSelect ── */
const OCI_SHAPES = [
  { value: "CI.Standard.A1.Flex", label: "ARM - Ampere A1", arch: "ARM" },
  { value: "CI.Standard.E4.Flex", label: "x86 - Standard E4", arch: "x86" },
];

function OciShapeSelect({ value, onChange }) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef(null);
  const dropdownRef = useRef(null);
  const [dropdownStyle, setDropdownStyle] = useState({});

  useEffect(() => {
    function handleClick(e) {
      if (
        triggerRef.current && !triggerRef.current.contains(e.target) &&
        dropdownRef.current && !dropdownRef.current.contains(e.target)
      ) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const openDropdown = () => {
    if (!triggerRef.current) return;
    const r = triggerRef.current.getBoundingClientRect();
    setDropdownStyle({ position: "fixed", top: r.bottom + 4, left: r.left, width: r.width });
    setOpen(true);
  };

  const selected = OCI_SHAPES.find((s) => s.value === value) || OCI_SHAPES[0];

  return (
    <div ref={triggerRef} style={{ position: "relative" }}>
      <button
        onClick={() => (open ? setOpen(false) : openDropdown())}
        style={{
          width: "100%",
          background: "var(--bg-surface)",
          border: "1px solid var(--border-accent)",
          borderRadius: 3,
          padding: "8px 10px",
          color: "var(--text-primary)",
          fontFamily: "inherit",
          fontSize: 12,
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
          textAlign: "left",
          outline: "none",
        }}
      >
        <span style={{ flex: 1, color: "var(--text-secondary)", fontWeight: 600 }}>{selected.label}</span>
        <span
          style={{
            display: "inline-block",
            width: 7,
            height: 7,
            borderRight: "1.5px solid var(--text-muted)",
            borderBottom: "1.5px solid var(--text-muted)",
            transform: open ? "rotate(225deg)" : "rotate(45deg)",
            position: "relative",
            top: open ? "2px" : "-2px",
            flexShrink: 0,
          }}
        />
      </button>
      {open && ReactDOM.createPortal(
        <div
          ref={dropdownRef}
          style={{
            ...dropdownStyle,
            background: "var(--bg-elevated)",
            border: "1px solid var(--border-strong)",
            borderRadius: 4,
            zIndex: 9999,
            boxShadow: "0 16px 48px rgba(0,0,0,0.7)",
            overflow: "hidden",
          }}
        >
          {OCI_SHAPES.map((s) => (
            <div
              key={s.value}
              onClick={() => { onChange(s.value); setOpen(false); }}
              style={{
                padding: "10px 12px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 10,
                background: s.value === (value || OCI_SHAPES[0].value) ? "rgba(0,200,224,0.08)" : "transparent",
                borderBottom: "1px solid rgba(255,255,255,0.04)",
              }}
              onMouseEnter={(e) => {
                if (s.value !== (value || OCI_SHAPES[0].value))
                  e.currentTarget.style.background = "var(--bg-surface)";
              }}
              onMouseLeave={(e) => {
                if (s.value !== (value || OCI_SHAPES[0].value))
                  e.currentTarget.style.background = "transparent";
              }}
            >
              <span style={{ flex: 1 }}>
                <span style={{
                  display: "block",
                  fontSize: 12,
                  fontWeight: 600,
                  color: s.value === (value || OCI_SHAPES[0].value) ? "var(--text-success)" : "var(--text-primary)",
                }}>
                  {s.label}
                </span>
                <span style={{ display: "block", fontSize: 10, color: "var(--text-dim)", marginTop: 1 }}>
                  ${OCI_SHAPE_PRICING[s.value].ocpuPrice.toFixed(2)}/OCPU/h · ${OCI_SHAPE_PRICING[s.value].memPrice.toFixed(3)}/GB/h
                </span>
              </span>
              {s.value === (value || OCI_SHAPES[0].value) && (
                <span style={{ color: "var(--text-success)", fontSize: 10, flexShrink: 0 }}>✓</span>
              )}
            </div>
          ))}
        </div>,
        document.body
      )}
    </div>
  );
}

/* ── InstancePicker ── */
const CAT_LABELS = {
  general: "General",
  compute: "Compute",
  memory: "Memory",
  gpu: "GPU",
  hpc: "HPC",
};
const CAT_COLORS = {
  general: "oklch(0.65 0.12 200)",
  compute: "oklch(0.65 0.14 150)",
  memory: "oklch(0.65 0.14 280)",
  gpu: "oklch(0.65 0.16 60)",
  hpc: "oklch(0.65 0.14 30)",
};

function InstancePicker({ value, onChange, defaultCat = "gpu" }) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [filterCat, setFilterCat] = useState(defaultCat);
  const [filterGpu, setFilterGpu] = useState(false);
  const [minVcpu, setMinVcpu] = useState("");
  const [minMem, setMinMem] = useState("");
  const triggerRef = useRef(null);
  const dropdownRef = useRef(null);
  const [dropdownStyle, setDropdownStyle] = useState({});
  const instances = window.SCALER_INSTANCES || [];

  const filtered = instances.filter((i) => {
    if (search && !i.type.toLowerCase().includes(search.toLowerCase()))
      return false;
    if (filterCat !== "all" && i.cat !== filterCat) return false;
    if (filterGpu && i.gpu === 0) return false;
    if (minVcpu && i.vcpu < parseInt(minVcpu)) return false;
    if (minMem && i.mem < parseFloat(minMem)) return false;
    return true;
  });

  useEffect(() => {
    function handleClick(e) {
      if (
        triggerRef.current &&
        !triggerRef.current.contains(e.target) &&
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target)
      )
        setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const openDropdown = () => {
    if (!triggerRef.current) return;
    const r = triggerRef.current.getBoundingClientRect();
    const vh = window.innerHeight;
    const vw = window.innerWidth;
    const gap = 4;
    const minW = Math.min(Math.max(540, r.width), vw - 8);
    const POPUP_H = 370;
    const spaceBelow = vh - r.bottom - gap;
    const spaceAbove = r.top - gap;
    const left = Math.min(r.left, vw - minW - 4);

    let style;
    if (spaceBelow >= POPUP_H || spaceBelow >= spaceAbove) {
      const availH = Math.max(120, spaceBelow - 90);
      style = {
        position: "fixed",
        top: r.bottom + gap,
        left,
        minWidth: minW,
        maxResultsH: Math.min(280, availH),
      };
    } else {
      const availH = Math.max(120, spaceAbove - 90);
      style = {
        position: "fixed",
        bottom: vh - r.top + gap,
        left,
        minWidth: minW,
        maxResultsH: Math.min(280, availH),
      };
    }
    setDropdownStyle(style);
    setOpen(true);
  };

  const selected = instances.find((i) => i.type === value);

  const dropdown = (
    <div
      ref={dropdownRef}
      style={{
        position: dropdownStyle.position,
        top: dropdownStyle.top,
        bottom: dropdownStyle.bottom,
        left: dropdownStyle.left,
        minWidth: dropdownStyle.minWidth,
        background: "var(--bg-elevated)",
        border: "1px solid var(--border-strong)",
        borderRadius: "4px",
        zIndex: 9999,
        boxShadow: "0 16px 48px rgba(0,0,0,0.7)",
      }}
    >
      {/* Filters */}
      <div
        style={{
          padding: "12px 14px 10px",
          borderBottom: "1px solid rgba(255,255,255,0.06)",
          display: "flex",
          flexWrap: "wrap",
          gap: "8px",
          alignItems: "center",
        }}
      >
        <input
          autoFocus
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search instance type…"
          style={{
            flex: "1 1 140px",
            background: "var(--bg-surface)",
            border: "1px solid rgba(255,255,255,0.1)",
            borderRadius: "3px",
            padding: "6px 9px",
            color: "var(--text-primary)",
            fontFamily: "inherit",
            fontSize: "12px",
            outline: "none",
          }}
        />
        <input
          value={minVcpu}
          onChange={(e) => setMinVcpu(e.target.value)}
          placeholder="Min vCPU"
          type="number"
          min={0}
          style={{
            width: 80,
            background: "var(--bg-surface)",
            border: "1px solid rgba(255,255,255,0.1)",
            borderRadius: "3px",
            padding: "6px 8px",
            color: "var(--text-primary)",
            fontFamily: "inherit",
            fontSize: "12px",
            outline: "none",
          }}
        />
        <input
          value={minMem}
          onChange={(e) => setMinMem(e.target.value)}
          placeholder="Min mem"
          type="number"
          min={0}
          style={{
            width: 80,
            background: "var(--bg-surface)",
            border: "1px solid rgba(255,255,255,0.1)",
            borderRadius: "3px",
            padding: "6px 8px",
            color: "var(--text-primary)",
            fontFamily: "inherit",
            fontSize: "12px",
            outline: "none",
          }}
        />
        <label
          style={{
            display: "flex",
            alignItems: "center",
            gap: 5,
            fontSize: 11,
            color: "var(--text-warning)",
            cursor: "pointer",
            userSelect: "none",
          }}
        >
          <input
            type="checkbox"
            checked={filterGpu}
            onChange={(e) => setFilterGpu(e.target.checked)}
            style={{ accentColor: "var(--text-warning)" }}
          />
          GPU only
        </label>
      </div>

      {/* Category tabs */}
      <div
        style={{
          display: "flex",
          gap: 0,
          borderBottom: "1px solid rgba(255,255,255,0.06)",
        }}
      >
        {["all", "general", "compute", "memory", "gpu", "hpc"].map((cat) => (
          <button
            key={cat}
            onClick={() => setFilterCat(cat)}
            style={{
              flex: 1,
              background:
                filterCat === cat ? "rgba(0,200,224,0.1)" : "transparent",
              border: "none",
              borderBottom:
                filterCat === cat
                  ? "2px solid var(--tab-active)"
                  : "2px solid transparent",
              color:
                filterCat === cat ? "var(--tab-active)" : "var(--text-muted)",
              fontFamily: "inherit",
              fontSize: "10px",
              padding: "7px 4px",
              cursor: "pointer",
            }}
          >
            {cat === "all" ? "All" : CAT_LABELS[cat]}
          </button>
        ))}
      </div>

      {/* Results */}
      <div
        style={{
          maxHeight: dropdownStyle.maxResultsH || 280,
          overflowY: "auto",
        }}
      >
        {filtered.length === 0 && (
          <div
            style={{
              padding: "20px",
              textAlign: "center",
              color: "var(--text-dim)",
              fontSize: 12,
            }}
          >
            No instances match
          </div>
        )}
        <table
          style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}
        >
          <thead>
            <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
              {["Instance", "vCPU", "Mem (GB)", "GPU", "Network", "USD/h"].map(
                (h) => (
                  <th
                    key={h}
                    style={{
                      padding: "6px 10px",
                      color: "var(--text-dim)",
                      fontWeight: 500,
                      textAlign: "left",
                      fontSize: 10,
                      letterSpacing: "0.05em",
                    }}
                  >
                    {h}
                  </th>
                ),
              )}
            </tr>
          </thead>
          <tbody>
            {filtered.map((i) => (
              <tr
                key={i.type}
                onClick={() => {
                  onChange(i.type);
                  setOpen(false);
                }}
                style={{
                  borderBottom: "1px solid rgba(255,255,255,0.03)",
                  cursor: "pointer",
                  background:
                    i.type === value
                      ? "rgba(0,200,224,0.08)"
                      : i.featured
                        ? "rgba(255,255,255,0.06)"
                        : "transparent",
                  transition: "background 0.1s",
                }}
                onMouseEnter={(e) => {
                  if (i.type !== value)
                    e.currentTarget.style.background = "rgba(255,255,255,0.08)";
                }}
                onMouseLeave={(e) => {
                  if (i.type !== value)
                    e.currentTarget.style.background = i.featured
                      ? "rgba(255,255,255,0.06)"
                      : "transparent";
                }}
              >
                <td
                  style={{
                    padding: "7px 10px",
                    fontWeight: 600,
                    color:
                      i.type === value
                        ? "var(--text-success)"
                        : "var(--text-primary)",
                  }}
                >
                  <span
                    style={{
                      fontSize: 10,
                      marginRight: 5,
                      color: CAT_COLORS[i.cat],
                    }}
                  >
                    ●
                  </span>
                  {i.type}
                </td>
                <td
                  style={{
                    padding: "7px 10px",
                    color: "var(--text-secondary)",
                    textAlign: "right",
                  }}
                >
                  {i.vcpu}
                </td>
                <td
                  style={{
                    padding: "7px 10px",
                    color: "var(--text-secondary)",
                    textAlign: "right",
                  }}
                >
                  {i.mem}
                </td>
                <td
                  style={{
                    padding: "7px 10px",
                    color:
                      i.gpu > 0 ? "var(--text-warning)" : "var(--text-dim)",
                  }}
                >
                  {i.gpu > 0 ? `${i.gpu}× ${i.gpuType} (${i.gpuMem}GB)` : "—"}
                </td>
                <td
                  style={{
                    padding: "7px 10px",
                    color: "var(--text-muted)",
                    fontSize: 11,
                  }}
                >
                  {i.net}
                </td>
                <td
                  style={{
                    padding: "7px 10px",
                    color: "var(--text-success)",
                    textAlign: "right",
                  }}
                >
                  {i.price.toFixed(2)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <div ref={triggerRef} style={{ position: "relative" }}>
      <button
        onClick={() => (open ? setOpen(false) : openDropdown())}
        style={{
          width: "100%",
          background: "var(--bg-surface)",
          border: "1px solid var(--border-accent)",
          borderRadius: "3px",
          padding: "9px 12px",
          color: value ? "var(--text-primary)" : "var(--text-muted)",
          fontFamily: "inherit",
          fontSize: "13px",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "8px",
          textAlign: "left",
        }}
      >
        <span style={{ flex: 1 }}>
          {value ? (
            <span>
              <span style={{ color: "var(--text-success)", fontWeight: 600 }}>
                {value}
              </span>
              {selected && (
                <span
                  style={{
                    color: "var(--text-muted)",
                    marginLeft: 10,
                    fontSize: 11,
                  }}
                >
                  {selected.vcpu} vCPU · {selected.mem} GB
                  {selected.gpu > 0
                    ? ` · ${selected.gpu}× ${selected.gpuType}`
                    : ""}
                </span>
              )}
            </span>
          ) : (
            <span>Select instance type…</span>
          )}
        </span>
        {selected && (
          <span
            style={{
              color: "var(--text-success)",
              fontSize: 11,
              flexShrink: 0,
            }}
          >
            USD {selected.price.toFixed(2)}/h
          </span>
        )}
        <span
          style={{
            display: "inline-block",
            width: 7,
            height: 7,
            borderRight: "1.5px solid var(--text-muted)",
            borderBottom: "1.5px solid var(--text-muted)",
            transform: open ? "rotate(225deg)" : "rotate(45deg)",
            position: "relative",
            top: open ? "2px" : "-2px",
            flexShrink: 0,
          }}
        />
      </button>
      {open && ReactDOM.createPortal(dropdown, document.body)}
    </div>
  );
}

/* ── TerminalWindow ── */
function TerminalWindow({ lines, config, style }) {
  const [displayed, setDisplayed] = useState([]);
  const endRef = useRef(null);

  useEffect(() => {
    if (!lines || lines.length === 0) return;
    setDisplayed([]);
    const timers = lines.map((line) => {
      const text = line.text
        .replace("{schedulerType}", config.schedulerType || "c5.xlarge")
        .replace("{workerType}", config.workerType || "c5.2xlarge")
        .replace("{region}", config.region || "us-east-1");
      return setTimeout(() => {
        setDisplayed((d) => [...d, { ...line, text }]);
      }, line.t);
    });
    return () => timers.forEach(clearTimeout);
  }, [lines]);

  useEffect(() => {
    if (endRef.current) endRef.current.scrollTop = endRef.current.scrollHeight;
  }, [displayed]);

  const clsColor = {
    dim: "var(--text-dim)",
    cmd: "var(--text-success)",
    ok: "var(--text-success)",
    info: "var(--text-secondary)",
    err: "var(--text-danger)",
    warn: "var(--text-warning)",
    done: "var(--text-success)",
    addr: "var(--text-accent)",
  };

  return (
    <div
      style={{
        background: "var(--term-bg)",
        border: "1px solid var(--term-border)",
        borderRadius: "4px",
        overflow: "hidden",
        ...style,
      }}
    >
      <div
        style={{
          background: "var(--term-titlebar)",
          borderBottom: "1px solid var(--term-border)",
          padding: "7px 14px",
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: "#ff5f57",
            display: "inline-block",
          }}
        ></span>
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: "#febc2e",
            display: "inline-block",
          }}
        ></span>
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: "#28c840",
            display: "inline-block",
          }}
        ></span>
        <span
          style={{
            marginLeft: 8,
            fontSize: 11,
            color: "var(--text-muted)",
          }}
        >
          openGRIS Scaler — deploy log
        </span>
      </div>
      <div
        ref={endRef}
        style={{
          padding: "14px 16px",
          fontFamily: "var(--font-mono)",
          fontSize: "12px",
          lineHeight: "1.7",
          minHeight: 400,
          maxHeight: 600,
          overflowY: "auto",
          color: "var(--text-secondary)",
        }}
      >
        {displayed.map((line, i) => (
          <div
            key={i}
            style={{
              color: clsColor[line.cls] || "var(--text-secondary)",
              fontWeight: line.cls === "done" ? 700 : 400,
              letterSpacing: line.cls === "done" ? "0.08em" : "normal",
            }}
          >
            {line.text}
          </div>
        ))}
        {displayed.length > 0 && displayed.length < (lines || []).length && (
          <span
            style={{
              color: "var(--text-success)",
              animation: "blink 1s step-end infinite",
            }}
          >
            ▌
          </span>
        )}
      </div>
    </div>
  );
}

/* ── DeployDetails ── */
function CopyButton({ value }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(value).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      });
    } else {
      const el = document.createElement("textarea");
      el.value = value;
      el.style.position = "fixed";
      el.style.opacity = "0";
      document.body.appendChild(el);
      el.select();
      document.execCommand("copy");
      document.body.removeChild(el);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    }
  };
  return (
    <button
      onClick={copy}
      title="Copy"
      style={{
        background: "none",
        border: "1px solid var(--border-accent)",
        borderRadius: 3,
        color: copied ? "var(--text-success)" : "var(--text-muted)",
        fontFamily: "inherit",
        fontSize: 10,
        padding: "2px 7px",
        cursor: "pointer",
        letterSpacing: "0.06em",
        transition: "color 0.15s, border-color 0.15s",
        flexShrink: 0,
      }}
    >
      {copied ? "COPIED" : "COPY"}
    </button>
  );
}

function DeployDetails({ visible, style }) {
  if (!visible) return null;
  const fields = [
    { label: "Scheduler Address", value: "54.211.148.92:8080", href: null },
    {
      label: "Worker Monitor Address",
      value: "http://54.211.148.92:3000",
      href: "http://54.211.148.92:3000",
    },
  ];
  return (
    <div
      style={{
        background: "rgba(0,255,136,0.03)",
        border: "1px solid var(--border-success)",
        borderRadius: "4px",
        padding: "20px 24px",
        ...style,
      }}
    >
      <div
        style={{
          fontSize: 11,
          color: "var(--text-success)",
          marginBottom: 14,
        }}
      >
        Deployment Details
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {fields.map(({ label, value, href }) => (
          <div
            key={label}
            style={{ display: "flex", alignItems: "center", gap: 12 }}
          >
            <span
              style={{
                fontSize: 11,
                color: "var(--text-dim)",
                letterSpacing: "0.05em",
                width: 140,
                flexShrink: 0,
              }}
            >
              {label}
            </span>
            {href ? (
              <a
                href={href}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  fontSize: 13,
                  color: "var(--text-accent)",
                  fontWeight: 500,
                  fontFamily: "var(--font-mono)",
                  textDecoration: "none",
                  borderBottom: "1px solid var(--border-accent)",
                }}
                onMouseEnter={(e) =>
                  (e.currentTarget.style.color = "var(--text-primary)")
                }
                onMouseLeave={(e) =>
                  (e.currentTarget.style.color = "var(--text-accent)")
                }
              >
                {value}
              </a>
            ) : (
              <span
                style={{
                  fontSize: 13,
                  color: "var(--text-primary)",
                  fontWeight: 500,
                  fontFamily: "var(--font-mono)",
                }}
              >
                {value}
              </span>
            )}
            <CopyButton value={value} />
          </div>
        ))}
      </div>
    </div>
  );
}

/* ── HelpTip ── */
function HelpTip({ text, children, width = 320 }) {
  const [btnRect, setBtnRect] = useState(null);
  const [measured, setMeasured] = useState(null); // null while measuring, then { above, width }
  const btnRef = useRef(null);
  const popupRef = useRef(null);
  const MAX_WIDTH = width;

  const open = btnRect !== null;

  useEffect(() => {
    if (!open) return;
    function handleClick(e) {
      if (btnRef.current && !btnRef.current.contains(e.target))
        setBtnRect(null);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  useEffect(() => {
    if (!open || !popupRef.current || measured !== null) return;
    const h = popupRef.current.offsetHeight;
    const w = popupRef.current.offsetWidth;
    setMeasured({ above: btnRect.top >= h + 16, width: w });
  }, [open, btnRect, measured]);

  const handleOpen = () => {
    if (!btnRef.current) return;
    setBtnRect(btnRef.current.getBoundingClientRect());
    setMeasured(null);
  };

  const renderBlock = (block, key) => {
    const lines = block.split("\n");
    if (lines.every((l) => l.startsWith("- "))) {
      return (
        <ul key={key} style={{ margin: 0, paddingLeft: 14 }}>
          {lines.map((l, i) => (
            <li key={i} style={{ marginTop: i > 0 ? 3 : 0 }}>
              {l.slice(2)}
            </li>
          ))}
        </ul>
      );
    }
    return (
      <p key={key} style={{ margin: 0 }}>
        {block}
      </p>
    );
  };

  const sections = text.split(/\n?---\n?/);
  const content = sections.map((section, si) => (
    <React.Fragment key={si}>
      {si > 0 && (
        <hr
          style={{
            border: "none",
            borderTop: "1px solid var(--border-accent)",
            margin: "8px 0",
          }}
        />
      )}
      {section.split(/\n\n+/).map((block, pi) => renderBlock(block, pi))}
    </React.Fragment>
  ));

  const popup =
    open &&
    (() => {
      const actualWidth = measured?.width ?? MAX_WIDTH;
      const left = Math.min(
        Math.max(8, btnRect.left + btnRect.width / 2 - actualWidth / 2),
        window.innerWidth - actualWidth - 8,
      );
      const above = measured?.above === true;
      const posStyle =
        measured === null
          ? { top: 0, visibility: "hidden" }
          : above
            ? { bottom: window.innerHeight - btnRect.top + 7 }
            : { top: btnRect.bottom + 7 };
      const arrowBorders = above
        ? { borderTop: "none", borderLeft: "none", bottom: -5 }
        : { borderBottom: "none", borderRight: "none", top: -5 };
      const arrowLeft = btnRect.left + btnRect.width / 2 - left - 4;

      return ReactDOM.createPortal(
        <div
          ref={popupRef}
          style={{
            position: "fixed",
            left,
            ...posStyle,
            width: "max-content",
            maxWidth: MAX_WIDTH,
            background: "var(--bg-popup)",
            border: "1px solid var(--border-strong)",
            borderRadius: 4,
            padding: "10px 12px",
            fontSize: 11,
            lineHeight: 1.65,
            color: "var(--text-secondary)",
            zIndex: 2000,
            boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
            pointerEvents: "none",
            textTransform: "none",
            letterSpacing: "normal",
            fontWeight: 400,
          }}
        >
          {content}
          {measured !== null && (
            <div
              style={{
                position: "absolute",
                left: arrowLeft,
                transform: "rotate(45deg)",
                width: 8,
                height: 8,
                background: "var(--bg-popup)",
                border: "1px solid var(--border-strong)",
                ...arrowBorders,
              }}
            />
          )}
        </div>,
        document.body,
      );
    })();

  if (children) {
    return (
      <span
        ref={btnRef}
        onMouseEnter={handleOpen}
        onMouseLeave={() => setBtnRect(null)}
        style={{ display: "inline-flex" }}
      >
        {children}
        {popup}
      </span>
    );
  }

  return (
    <span style={{ display: "inline-flex", alignItems: "center" }}>
      <button
        ref={btnRef}
        onMouseEnter={handleOpen}
        onMouseLeave={() => setBtnRect(null)}
        onClick={() => (open ? setBtnRect(null) : handleOpen())}
        style={{
          width: 15,
          height: 15,
          borderRadius: "50%",
          border: "1px solid var(--border-strong)",
          background: open ? "var(--bg-surface)" : "rgba(0,200,224,0.05)",
          color: "var(--accent-cyan)",
          fontFamily: "inherit",
          fontSize: 9,
          fontWeight: 700,
          cursor: "pointer",
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          flexShrink: 0,
          transition: "background 0.15s, border-color 0.15s",
          lineHeight: 1,
        }}
      >
        ?
      </button>
      {popup}
    </span>
  );
}

/* ── LiveTerminal ── */
function LiveTerminal({ lines, isRunning, title, style, bare }) {
  const scrollRef = useRef(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const isNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
    if (isNearBottom) el.scrollTop = el.scrollHeight;
  }, [lines]);

  const clsColor = {
    dim: "var(--text-dim)",
    cmd: "var(--text-success)",
    ok: "var(--text-success)",
    info: "var(--text-secondary)",
    err: "var(--text-danger)",
    warn: "var(--text-warning)",
    done: "var(--text-success)",
    addr: "var(--text-accent)",
  };

  const content = (
    <div
      ref={scrollRef}
      style={{
        padding: bare ? "0" : "14px 16px",
        fontFamily: "var(--font-mono)",
        fontSize: "12px",
        lineHeight: "1.7",
        flex: 1,
        overflowY: "auto",
        color: "var(--text-secondary)",
      }}
    >
      {lines.map((line, i) => (
        <div
          key={i}
          style={{
            color: clsColor[line.cls] || "var(--text-secondary)",
            fontWeight: line.cls === "done" ? 700 : 400,
            letterSpacing: line.cls === "done" ? "0.08em" : "normal",
            whiteSpace: "pre-wrap",
            wordBreak: "break-all",
          }}
        >
          {line.text}
        </div>
      ))}
      {isRunning && (
        <span
          style={{
            color: "var(--text-success)",
            animation: "blink 1s step-end infinite",
          }}
        >
          ▌
        </span>
      )}
    </div>
  );

  if (bare) {
    return (
      <div style={{ display: "flex", flexDirection: "column", ...style }}>
        {content}
      </div>
    );
  }

  return (
    <div
      style={{
        background: "var(--term-bg)",
        border: "1px solid var(--term-border)",
        borderRadius: "4px",
        display: "flex",
        flexDirection: "column",
        ...style,
      }}
    >
      <div
        style={{
          background: "var(--term-titlebar)",
          borderBottom: "1px solid var(--term-border)",
          padding: "7px 14px",
          display: "flex",
          alignItems: "center",
          gap: 8,
          flexShrink: 0,
        }}
      >
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: "#ff5f57",
            display: "inline-block",
          }}
        />
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: "#febc2e",
            display: "inline-block",
          }}
        />
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: "#28c840",
            display: "inline-block",
          }}
        />
        <span
          style={{
            marginLeft: 8,
            fontSize: 11,
            color: "var(--text-muted)",
          }}
        >
          {title || "openGRIS Scaler — deploy log"}
        </span>
      </div>
      {content}
    </div>
  );
}

/* ── SchedulerLogTerminal ── */
const POLL_INTERVALS = [
  { label: "15s", value: 15000 },
  { label: "30s", value: 30000 },
  { label: "1m", value: 60000 },
  { label: "5m", value: 300000 },
];

function SchedulerLogTerminal({ instanceId, region, credentials, isActive }) {
  const [lines, setLines] = useState([]);
  const [status, setStatus] = useState("idle");
  const [errorMsg, setError] = useState(null);
  const [intervalMs, setIntervalMs] = useState(15000);
  const [nextFetchAt, setNextFetchAt] = useState(null);
  const [countdown, setCountdown] = useState(null);
  const [fetching, setFetching] = useState(false);
  const pollRef = useRef(null);
  const triggerRef = useRef(null);
  const intervalMsRef = useRef(intervalMs);
  useEffect(() => {
    intervalMsRef.current = intervalMs;
  }, [intervalMs]);

  const fetchLogs = useCallback(async () => {
    try {
      const ssm = new AWS.SSM({
        region,
        credentials: new AWS.Credentials(
          credentials.accessKeyId,
          credentials.secretKey,
        ),
      });
      let commandId;
      try {
        const r = await ssm
          .sendCommand({
            InstanceIds: [instanceId],
            DocumentName: "AWS-RunShellScript",
            Parameters: {
              commands: [
                "tail -n 150 /var/log/scaler.log 2>/dev/null || echo '(log not yet available)'",
              ],
            },
            TimeoutSeconds: 30,
          })
          .promise();
        commandId = r.Command.CommandId;
      } catch (err) {
        if (err.code === "InvalidInstanceId") {
          setLines([
            {
              text: "Instance not yet registered with SSM — retrying…",
              cls: "warn",
            },
          ]);
        } else if (err.code === "AccessDeniedException") {
          setStatus("error");
          setError(
            "Permission denied. Your IAM user needs ssm:SendCommand and ssm:GetCommandInvocation.",
          );
        } else if (
          err.code === "InvalidClientTokenId" ||
          err.code === "AuthFailure" ||
          /invalid.*token/i.test(err.message)
        ) {
          setStatus("error");
          setError(
            "Invalid AWS credentials. Re-enter your Access Key ID and Secret Access Key in the Configuration tab.",
          );
        } else {
          setStatus("error");
          setError("SSM error: " + err.message);
        }
        return;
      }
      for (let i = 0; i < 12; i++) {
        await new Promise((r) => setTimeout(r, 2500));
        try {
          const inv = await ssm
            .getCommandInvocation({
              CommandId: commandId,
              InstanceId: instanceId,
            })
            .promise();
          if (inv.Status === "Success" || inv.Status === "Failed") {
            setLines(
              (inv.StandardOutputContent || "")
                .split("\n")
                .map((text) => ({ text, cls: "info" })),
            );
            break;
          }
        } catch (_) {
          break;
        }
      }
    } catch (_) {}
  }, [instanceId, region, credentials]);

  const hasCredentials = credentials.accessKeyId && credentials.secretKey;

  useEffect(() => {
    if (!isActive || !instanceId || !hasCredentials) return;
    setStatus("polling");
    let cancelled = false;

    const run = async () => {
      if (cancelled) return;
      setFetching(true);
      setNextFetchAt(null);
      await fetchLogs();
      if (cancelled) return;
      const ms = intervalMsRef.current;
      const next = Date.now() + ms;
      setFetching(false);
      setNextFetchAt(next);
      setCountdown(Math.round(ms / 1000));
      pollRef.current = setTimeout(run, ms);
    };

    triggerRef.current = () => {
      clearTimeout(pollRef.current);
      run();
    };

    run();
    return () => {
      cancelled = true;
      clearTimeout(pollRef.current);
      triggerRef.current = null;
    };
  }, [isActive, instanceId, hasCredentials, fetchLogs, intervalMs]);

  useEffect(() => {
    if (!nextFetchAt) {
      setCountdown(null);
      return;
    }
    const tick = setInterval(() => {
      setCountdown(Math.max(0, Math.round((nextFetchAt - Date.now()) / 1000)));
    }, 1000);
    return () => clearInterval(tick);
  }, [nextFetchAt]);

  if (!hasCredentials)
    return (
      <div
        style={{ padding: 24, color: "var(--text-secondary)", fontSize: 12 }}
      >
        Re-enter your AWS credentials in the Configuration tab to view scheduler
        logs.
      </div>
    );
  if (status === "error")
    return (
      <div style={{ padding: 24, color: "var(--text-danger)", fontSize: 12 }}>
        {errorMsg}
      </div>
    );

  const labelMs =
    POLL_INTERVALS.find((o) => o.value === intervalMs)?.label ?? "15s";

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        flex: 1,
        minHeight: 0,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          padding: "0 0 10px 0",
          flexShrink: 0,
        }}
      >
        <span
          style={{
            fontSize: 11,
            color: "var(--text-muted)",
            fontFamily: "var(--font-mono)",
          }}
        >
          /var/log/scaler.log
        </span>
        <span style={{ fontSize: 11, color: "var(--text-dim)" }}>·</span>
        <span
          style={{
            fontSize: 11,
            color: "var(--text-muted)",
            display: "flex",
            alignItems: "center",
            gap: 6,
          }}
        >
          refresh every
          <select
            value={intervalMs}
            onChange={(e) => setIntervalMs(Number(e.target.value))}
            style={{
              background: "var(--bg-input, var(--term-bg))",
              border: "1px solid var(--border-subtle, var(--term-border))",
              borderRadius: 3,
              color: "var(--text-secondary)",
              fontSize: 11,
              padding: "1px 4px",
              cursor: "pointer",
            }}
          >
            {POLL_INTERVALS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </span>
        {(fetching || countdown !== null) && (
          <>
            <span style={{ fontSize: 11, color: "var(--text-dim)" }}>·</span>
            <span style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {fetching ? "refreshing…" : `next refresh in ${countdown}s`}
            </span>
          </>
        )}
        <button
          onClick={() => triggerRef.current?.()}
          disabled={fetching}
          style={{
            marginLeft: "auto",
            background: "none",
            border: "1px solid var(--border-accent)",
            borderRadius: 3,
            color: fetching ? "var(--text-dim)" : "var(--text-muted)",
            fontFamily: "inherit",
            fontSize: 11,
            padding: "2px 8px",
            cursor: fetching ? "default" : "pointer",
            letterSpacing: "0.06em",
          }}
        >
          Refresh
        </button>
      </div>
      <LiveTerminal
        lines={lines}
        isRunning={status === "polling"}
        bare
        style={{ flex: 1, minHeight: 0 }}
      />
    </div>
  );
}

/* ── WorkerManagerTypeSelect ── */
const WM_TYPE_DEFS = [
  {
    value: "orb_aws_ec2",
    label: "AWS EC2",
    badge: "AWS",
    desc: "Managed EC2 instances via ORB worker manager",
  },
  {
    value: "oci_raw",
    label: "OCI Container Instance",
    badge: "OCI",
    desc: "Oracle Cloud Infrastructure - container instances via OCIR",
  },
  {
    value: "aws_raw_ecs",
    label: "AWS ECS",
    badge: "AWS",
    desc: "Container tasks on Elastic Container Service",
    disabled: true,
  },
  {
    value: "aws_hpc",
    label: "AWS Batch",
    badge: "AWS",
    desc: "High-performance compute via AWS Batch",
    disabled: true,
  },
  {
    value: "symphony",
    label: "IBM Spectrum Symphony",
    badge: "IBM",
    desc: "IBM Spectrum Symphony grid via soamapi",
    disabled: true,
  },
  {
    value: "oci_hpc",
    label: "OCI HPC",
    badge: "OCI",
    desc: "Oracle Cloud Infrastructure - per-task container instance jobs",
    disabled: true,
  },
];

function WorkerManagerTypeSelect({ value, onChange }) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef(null);
  const dropdownRef = useRef(null);
  const [dropdownStyle, setDropdownStyle] = useState({});

  useEffect(() => {
    function handleClick(e) {
      if (
        triggerRef.current &&
        !triggerRef.current.contains(e.target) &&
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target)
      )
        setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const openDropdown = () => {
    if (!triggerRef.current) return;
    const r = triggerRef.current.getBoundingClientRect();
    setDropdownStyle({
      position: "fixed",
      top: r.bottom + 4,
      left: r.left,
      minWidth: r.width,
    });
    setOpen(true);
  };

  const selected = WM_TYPE_DEFS.find((t) => t.value === value);

  return (
    <div ref={triggerRef} style={{ position: "relative", flex: 1 }}>
      <button
        onClick={() => (open ? setOpen(false) : openDropdown())}
        style={{
          width: "100%",
          background: "var(--bg-surface)",
          border: "1px solid var(--border-accent)",
          borderRadius: 3,
          padding: "5px 8px",
          color: "var(--text-primary)",
          fontFamily: "inherit",
          fontSize: 11,
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 6,
          textAlign: "left",
          outline: "none",
        }}
      >
        <span
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            flex: 1,
            overflow: "hidden",
          }}
        >
          {selected && (
            <span
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.06em",
                color: "var(--text-accent)",
                background: "rgba(0,200,224,0.12)",
                border: "1px solid rgba(0,200,224,0.2)",
                borderRadius: 2,
                padding: "1px 5px",
                flexShrink: 0,
              }}
            >
              {selected.badge}
            </span>
          )}
          <span
            style={{
              color: "var(--text-primary)",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
            }}
          >
            {selected ? selected.label : "Select type…"}
          </span>
        </span>
        <span
          style={{
            display: "inline-block",
            width: 7,
            height: 7,
            borderRight: "1.5px solid var(--text-muted)",
            borderBottom: "1.5px solid var(--text-muted)",
            transform: open ? "rotate(225deg)" : "rotate(45deg)",
            position: "relative",
            top: open ? "2px" : "-2px",
            flexShrink: 0,
          }}
        />
      </button>
      {open &&
        ReactDOM.createPortal(
          <div
            ref={dropdownRef}
            style={{
              ...dropdownStyle,
              background: "var(--bg-elevated)",
              border: "1px solid var(--border-strong)",
              borderRadius: 4,
              zIndex: 9999,
              boxShadow: "0 16px 48px rgba(0,0,0,0.7)",
              overflow: "hidden",
            }}
          >
            {WM_TYPE_DEFS.map((t) => (
              <div
                key={t.value}
                onClick={() => {
                  if (!t.disabled) {
                    onChange(t.value);
                    setOpen(false);
                  }
                }}
                style={{
                  padding: "9px 12px",
                  cursor: t.disabled ? "not-allowed" : "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: 10,
                  opacity: t.disabled ? 0.4 : 1,
                  background:
                    t.value === value ? "rgba(0,200,224,0.08)" : "transparent",
                  borderBottom: "1px solid rgba(255,255,255,0.04)",
                }}
                onMouseEnter={(e) => {
                  if (!t.disabled && t.value !== value)
                    e.currentTarget.style.background = "var(--bg-surface)";
                }}
                onMouseLeave={(e) => {
                  if (!t.disabled && t.value !== value)
                    e.currentTarget.style.background = "transparent";
                }}
              >
                <span
                  style={{
                    fontSize: 9,
                    fontWeight: 700,
                    letterSpacing: "0.06em",
                    color:
                      t.value === value
                        ? "var(--text-success)"
                        : "var(--text-accent)",
                    background:
                      t.value === value
                        ? "rgba(0,255,136,0.1)"
                        : "rgba(0,200,224,0.1)",
                    border: `1px solid ${t.value === value ? "rgba(0,255,136,0.2)" : "rgba(0,200,224,0.18)"}`,
                    borderRadius: 2,
                    padding: "1px 5px",
                    flexShrink: 0,
                    minWidth: 34,
                    textAlign: "center",
                  }}
                >
                  {t.badge}
                </span>
                <span style={{ flex: 1 }}>
                  <span
                    style={{
                      display: "block",
                      fontSize: 12,
                      color:
                        t.value === value
                          ? "var(--text-success)"
                          : "var(--text-primary)",
                      fontWeight: 600,
                    }}
                  >
                    {t.label}
                  </span>
                  <span
                    style={{
                      display: "block",
                      fontSize: 10,
                      color: "var(--text-dim)",
                      marginTop: 1,
                    }}
                  >
                    {t.desc}
                  </span>
                </span>
                {t.value === value && (
                  <span
                    style={{
                      color: "var(--text-success)",
                      fontSize: 10,
                      flexShrink: 0,
                    }}
                  >
                    ✓
                  </span>
                )}
                {t.disabled && (
                  <span
                    style={{
                      color: "var(--text-dim)",
                      fontSize: 9,
                      flexShrink: 0,
                      fontStyle: "italic",
                    }}
                  >
                    Soon
                  </span>
                )}
              </div>
            ))}
          </div>,
          document.body,
        )}
    </div>
  );
}

Object.assign(window, {
  SecretInput,
  RegionSelect,
  OciRegionSelect,
  OciShapeSelect,
  InstancePicker,
  TerminalWindow,
  DeployDetails,
  HelpTip,
  LiveTerminal,
  SchedulerLogTerminal,
  WorkerManagerTypeSelect,
});
