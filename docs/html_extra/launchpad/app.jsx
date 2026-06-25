const { useState, useEffect, useCallback, useRef } = React;

const IS_ADVANCED = new URLSearchParams(window.location.search).has('advanced');
const IS_DEV = new URLSearchParams(window.location.search).has('dev');

const OCI_SHAPE_PRICING = {
  "CI.Standard.A1.Flex": { ocpuPrice: 0.013106, memPrice: 0.0019659 },
  "CI.Standard.E4.Flex": { ocpuPrice: 0.032765, memPrice: 0.0019659 },
};

/* ── NumericStepper ── */
function NumericStepper({
  value,
  onChange,
  min = 0,
  max = Infinity,
  step = 1,
  width = 56,
}) {
  const [hov, setHov] = useState(null);
  const [localValue, setLocalValue] = useState(null);
  const btnStyle = (side) => ({
    width: 28,
    height: "100%",
    background: "transparent",
    border: "none",
    borderLeft: side === "plus" ? "1px solid var(--border-accent)" : "none",
    borderRight: side === "minus" ? "1px solid var(--border-accent)" : "none",
    color: "var(--accent-cyan)",
    fontFamily: "inherit",
    fontSize: 16,
    lineHeight: 1,
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    flexShrink: 0,
    transition: "background 0.12s, color 0.12s",
    padding: 0,
  });
  return (
    <div
      style={{
        display: "flex",
        alignItems: "stretch",
        width: "fit-content",
        background: "var(--bg-surface)",
        border: "1px solid var(--border-accent)",
        borderRadius: 3,
        height: 36,
        overflow: "hidden",
      }}
    >
      <button
        style={{
          ...btnStyle("minus"),
          background: hov === "minus" ? "rgba(0,200,224,0.1)" : "transparent",
        }}
        onMouseEnter={() => setHov("minus")}
        onMouseLeave={() => setHov(null)}
        onClick={() => onChange(Math.max(min, value - step))}
      >
        −
      </button>
      <input
        type="number"
        value={localValue !== null ? localValue : value}
        onFocus={() => setLocalValue(String(value))}
        onChange={(e) => setLocalValue(e.target.value)}
        onBlur={() => {
          const v = parseFloat(localValue);
          if (!isNaN(v)) onChange(Math.min(max, Math.max(min, Math.round(v))));
          setLocalValue(null);
        }}
        style={{
          width,
          background: "transparent",
          border: "none",
          outline: "none",
          color: "var(--text-primary)",
          fontFamily: "inherit",
          fontSize: 13,
          fontWeight: 600,
          textAlign: "center",
          padding: "0 4px",
        }}
      />
      <button
        style={{
          ...btnStyle("plus"),
          background: hov === "plus" ? "rgba(0,200,224,0.1)" : "transparent",
        }}
        onMouseEnter={() => setHov("plus")}
        onMouseLeave={() => setHov(null)}
        onClick={() => onChange(Math.min(max, value + step))}
      >
        +
      </button>
    </div>
  );
}

/* ── PanelBox ── */
function PanelBox({ title, children, style }) {
  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--border-accent)",
        borderRadius: 6,
        padding: "20px 22px",
        display: "flex",
        flexDirection: "column",
        gap: 14,
        ...style,
      }}
    >
      {title && (
        <div
          style={{
            fontSize: 11,
            color: "var(--accent-cyan)",
            borderBottom: "1px solid var(--border-accent)",
            paddingBottom: 10,
            marginBottom: 2,
            fontWeight: 600,
          }}
        >
          {title}
        </div>
      )}
      {children}
    </div>
  );
}

/* ── WorkerManagerCard ── */
function WorkerManagerCard({
  wm,
  onChange,
  onRemove,
  allInstances,
  canRemove,
  fullWidth,
}) {
  const [localId, setLocalId] = useState(wm.id);
  useEffect(() => {
    setLocalId(wm.id);
  }, [wm.id]);

  const Label = ({ children, help }) => (
    <div
      style={{
        fontSize: 11,
        color: "var(--text-label)",
        marginBottom: 5,
        display: "flex",
        alignItems: "center",
        gap: 6,
      }}
    >
      <span>{children}</span>
      {help && <HelpTip text={help} />}
    </div>
  );
  const inp = {
    width: "100%",
    background: "var(--bg-surface)",
    border: "1px solid var(--border-accent)",
    borderRadius: 3,
    padding: "7px 10px",
    color: "var(--text-primary)",
    fontFamily: "inherit",
    fontSize: 12,
    outline: "none",
  };
  const set = (k, v) => onChange({ ...wm, [k]: v });

  const ToggleRow = ({ options, value, onChange: onTog }) => (
    <div
      style={{
        display: "flex",
        borderRadius: 3,
        overflow: "hidden",
        border: "1px solid var(--border-accent)",
      }}
    >
      {options.map(([val, lbl, disabled]) => (
        <button
          key={val}
          disabled={!!disabled}
          onClick={() => !disabled && onTog(val)}
          style={{
            flex: 1,
            padding: "6px 0",
            fontFamily: "inherit",
            fontSize: 11,
            cursor: disabled ? "not-allowed" : "pointer",
            border: "none",
            background: value === val ? "rgba(0,200,224,0.18)" : "transparent",
            color: disabled
              ? "var(--text-dim)"
              : value === val
                ? "var(--text-accent)"
                : "var(--text-muted)",
            transition: "background 0.15s, color 0.15s",
          }}
        >
          {lbl}
        </button>
      ))}
    </div>
  );

  const workerInst = allInstances.find((i) => i.type === wm.instanceType) || {
    price: 0,
  };
  const derivedCount =
    wm.capMode === "instances"
      ? Math.max(0, wm.instanceCap || 0)
      : Math.max(0, Math.floor((wm.budgetCap || 0) / (workerInst.price || 1)));
  const costPerHr = derivedCount * workerInst.price;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 12,
        width: fullWidth ? "100%" : 340,
        flexShrink: fullWidth ? 1 : 0,
      }}
    >
      {/* header */}
      <div style={{ display: "flex", alignItems: "flex-end", gap: 8 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <Label>Type</Label>
          <WorkerManagerTypeSelect
            value={wm.type}
            onChange={(v) => {
              if (v === "oci_raw") {
                onChange({ ...wm, type: v, ociShape: "CI.Standard.A1.Flex", ociContainerImage: "ghcr.io/finos/scaler:latest-arm64", capMode: "instances", instanceCap: 4, budgetCap: 10, ociOcpus: 4, ociMemoryGb: 8 });
              } else {
                set("type", v);
              }
            }}
          />
        </div>
        <div
          style={{ display: "flex", flexDirection: "column", gap: 4, flex: 1 }}
        >
          <Label help="Unique name for this worker manager.">Name</Label>
          <input
            value={localId}
            onChange={(e) => setLocalId(e.target.value)}
            onBlur={() => {
              const v = localId.trim();
              if (!v) setLocalId(wm.id);
              else if (v !== wm.id) set("id", v);
            }}
            placeholder="wm-id"
            style={{
              width: "100%",
              background: "var(--bg-surface)",
              border: "1px solid var(--border-accent)",
              borderRadius: 3,
              padding: "5px 8px",
              color: "var(--text-primary)",
              fontFamily: "inherit",
              fontSize: 11,
              outline: "none",
            }}
          />
        </div>
      </div>

      {/* orb_aws_ec2 */}
      {wm.type === "orb_aws_ec2" && (
        <>
          <div>
            <Label>Worker Instance Type</Label>
            <InstancePicker
              value={wm.instanceType}
              onChange={(v) => set("instanceType", v)}
              defaultCat="all"
            />
          </div>
          <div>
            <Label>Budget</Label>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {wm.capMode === "instances" ? (
                <NumericStepper
                  value={wm.instanceCap || 1}
                  onChange={(v) => set("instanceCap", v)}
                  min={1}
                  max={1000}
                />
              ) : (
                <NumericStepper
                  value={wm.budgetCap || 10}
                  onChange={(v) => set("budgetCap", v)}
                  min={0}
                  step={0.5}
                  width={64}
                />
              )}
              <select
                value={wm.capMode}
                onChange={(e) => set("capMode", e.target.value)}
                style={{
                  background: "var(--bg-surface)",
                  border: "1px solid var(--border-accent)",
                  borderRadius: 3,
                  padding: "6px 8px",
                  color: "var(--text-primary)",
                  fontFamily: "inherit",
                  fontSize: 11,
                  outline: "none",
                  cursor: "pointer",
                }}
              >
                <option value="budget">USD/h cap</option>
                <option value="instances">instance cap</option>
              </select>
            </div>
          </div>
          <div>
            <Label
              help={
                "- Installed on each worker instance\n- opengris-scaler must be included"
              }
            >
              requirements.txt
            </Label>
            <textarea
              value={wm.requirements}
              onChange={(e) => set("requirements", e.target.value)}
              spellCheck={false}
              style={{
                width: "100%",
                background: "var(--bg-surface)",
                border: "1px solid var(--border-accent)",
                borderRadius: 3,
                padding: "7px 10px",
                color: "var(--text-primary)",
                fontFamily: "inherit",
                fontSize: 11,
                outline: "none",
                resize: "vertical",
                minHeight: 72,
                lineHeight: 1.6,
              }}
            />
          </div>
          <div
            style={{
              padding: "8px 10px",
              background: "rgba(0,255,136,0.04)",
              border: "1px solid var(--border-success)",
              borderRadius: 3,
              display: "flex",
              justifyContent: "space-between",
              alignItems: "baseline",
            }}
          >
            <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
              Cost
            </span>
            <span
              style={{
                fontSize: 13,
                fontWeight: 600,
                color: "var(--text-success)",
              }}
            >
              USD {costPerHr.toFixed(2)}/h
            </span>
          </div>
        </>
      )}

      {/* aws_raw_ecs */}
      {wm.type === "aws_raw_ecs" && (
        <>
          <div>
            <Label>ECS Cluster</Label>
            <input
              value={wm.ecsCluster || ""}
              onChange={(e) => set("ecsCluster", e.target.value)}
              style={inp}
              placeholder="scaler-cluster"
            />
          </div>
          <div>
            <Label>Container Image</Label>
            <input
              value={wm.ecsTaskImage || ""}
              onChange={(e) => set("ecsTaskImage", e.target.value)}
              style={inp}
              placeholder="public.ecr.aws/v4u8j8r6/scaler:latest"
            />
          </div>
          <div>
            <Label>Subnets (comma-separated)</Label>
            <input
              value={wm.ecsSubnets || ""}
              onChange={(e) => set("ecsSubnets", e.target.value)}
              style={inp}
              placeholder="subnet-abc123, subnet-def456"
            />
          </div>
          <div>
            <Label>Task Definition</Label>
            <input
              value={wm.ecsTaskDefinition || ""}
              onChange={(e) => set("ecsTaskDefinition", e.target.value)}
              style={inp}
              placeholder="scaler-task-definition"
            />
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label>vCPU</Label>
              <NumericStepper
                value={wm.ecsTaskCpu || 4}
                onChange={(v) => set("ecsTaskCpu", v)}
                min={1}
                max={64}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label>Memory (GB)</Label>
              <NumericStepper
                value={wm.ecsTaskMemory || 30}
                onChange={(v) => set("ecsTaskMemory", v)}
                min={1}
                max={512}
              />
            </div>
          </div>
        </>
      )}

      {/* aws_hpc */}
      {wm.type === "aws_hpc" && (
        <>
          <div>
            <Label>Job Queue</Label>
            <input
              value={wm.jobQueue || ""}
              onChange={(e) => set("jobQueue", e.target.value)}
              style={inp}
              placeholder="scaler-batch-queue"
            />
          </div>
          <div>
            <Label>Job Definition</Label>
            <input
              value={wm.jobDefinition || ""}
              onChange={(e) => set("jobDefinition", e.target.value)}
              style={inp}
              placeholder="scaler-job-definition"
            />
          </div>
          <div>
            <Label>S3 Bucket</Label>
            <input
              value={wm.s3Bucket || ""}
              onChange={(e) => set("s3Bucket", e.target.value)}
              style={inp}
              placeholder="my-scaler-bucket"
            />
          </div>
          <div>
            <Label>S3 Prefix</Label>
            <input
              value={wm.s3Prefix || "scaler-tasks"}
              onChange={(e) => set("s3Prefix", e.target.value)}
              style={inp}
              placeholder="scaler-tasks"
            />
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label>Max Concurrent Jobs</Label>
              <NumericStepper
                value={wm.maxConcurrentJobs || 100}
                onChange={(v) => set("maxConcurrentJobs", v)}
                min={1}
                max={10000}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label>Timeout (min)</Label>
              <NumericStepper
                value={wm.jobTimeoutMinutes || 60}
                onChange={(v) => set("jobTimeoutMinutes", v)}
                min={1}
                max={1440}
              />
            </div>
          </div>
        </>
      )}

      {/* symphony */}
      {wm.type === "symphony" && (
        <>
          <div>
            <Label>Service Name</Label>
            <input
              value={wm.serviceName || ""}
              onChange={(e) => set("serviceName", e.target.value)}
              style={inp}
              placeholder="my-symphony-service"
            />
          </div>
        </>
      )}

      {/* oci_raw */}
      {wm.type === "oci_raw" && (() => {
        const ociShape = wm.ociShape || "CI.Standard.A1.Flex";
        const ociPricing = OCI_SHAPE_PRICING[ociShape] || OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
        const ociCostPerHr = ociPricing.ocpuPrice * (wm.ociOcpus || 4) + ociPricing.memPrice * (wm.ociMemoryGb || 8);
        const derivedCount = wm.capMode === "instances"
          ? Math.max(0, wm.instanceCap || 0)
          : Math.max(0, Math.floor((wm.budgetCap || 0) / (ociCostPerHr || 1)));
        return (
          <>
            <div>
              <Label help={"The OCID of the compartment where container instances will be launched. In the OCI Console, go to Identity & Security > Compartments, click your compartment, and copy the OCID. To use the root compartment, use your Tenancy OCID (ocid1.tenancy.oc1...) directly."}>Compartment ID</Label>
              <input
                value={wm.ociCompartmentId || ""}
                onChange={(e) => set("ociCompartmentId", e.target.value)}
                style={inp}
                placeholder="ocid1.compartment.oc1... or ocid1.tenancy.oc1..."
              />
            </div>
            <div>
              <Label help={"The availability domain where instances will run. In the OCI Console, go to Compute > Instances — the full AD name is in the AD column (e.g. Uocm:PHX-AD-1). If you have no instances yet, click Create Instance and check the Placement section to see the available ADs for your region."}>Availability Domain</Label>
              <input
                value={wm.ociAvailabilityDomain || ""}
                onChange={(e) => set("ociAvailabilityDomain", e.target.value)}
                style={inp}
                placeholder="AD-1"
              />
            </div>
            <div>
              <Label help={"The OCID of the subnet for container network interfaces. In the OCI Console, go to Networking > Virtual Cloud Networks > [your VCN] > Subnets, then click a subnet and copy its OCID.\n\nIf you don't have a VCN yet: go to Networking > Virtual Cloud Networks > Create VCN, and choose 'Create VCN with Internet Connectivity' to set up a VCN and public subnet automatically."}>Subnet ID</Label>
              <input
                value={wm.ociSubnetId || ""}
                onChange={(e) => set("ociSubnetId", e.target.value)}
                style={inp}
                placeholder="ocid1.subnet.oc1..aaa..."
              />
            </div>
            <div>
              <Label help={"The OCI region where your resources are located. To confirm the identifier in the OCI Console, click the region selector at the top of the page, then Manage Regions — identifiers are shown in the Region Identifier column."}>Region</Label>
              <OciRegionSelect
                value={wm.ociRegion || ""}
                onChange={(v) => set("ociRegion", v)}
              />
            </div>
            <div>
              <Label help="Instance shape determines the CPU architecture and pricing.">Instance Shape</Label>
              <OciShapeSelect
                value={ociShape}
                onChange={(v) => {
                  const image = v === "CI.Standard.A1.Flex"
                    ? "ghcr.io/finos/scaler:latest-arm64"
                    : "ghcr.io/finos/scaler:latest-amd64";
                  onChange({ ...wm, ociShape: v, ociContainerImage: image });
                }}
              />
            </div>
            <div style={{ display: "flex", gap: 10 }}>
              <div style={{ flex: 1 }}>
                <Label help="Number of OCPUs per container instance (also determines worker count).">OCPUs</Label>
                <NumericStepper
                  value={wm.ociOcpus || 4}
                  onChange={(v) => set("ociOcpus", v)}
                  min={1}
                  max={64}
                />
              </div>
              <div style={{ flex: 1 }}>
                <Label help="Memory in GB for the container instance. Must satisfy OCI's minimum memory-per-OCPU ratio for your chosen shape.">Memory (GB)</Label>
                <NumericStepper
                  value={wm.ociMemoryGb || 8}
                  onChange={(v) => set("ociMemoryGb", v)}
                  min={1}
                  max={512}
                />
              </div>
            </div>
            <div>
              <Label help="Maximum number of OCI container instances to run simultaneously. In budget mode, the cap is derived from your hourly USD budget divided by the per-instance cost.">Budget</Label>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                {wm.capMode === "instances" ? (
                  <NumericStepper
                    value={wm.instanceCap || 1}
                    onChange={(v) => set("instanceCap", v)}
                    min={1}
                    max={1000}
                  />
                ) : (
                  <NumericStepper
                    value={wm.budgetCap || 10}
                    onChange={(v) => set("budgetCap", v)}
                    min={0}
                    step={0.5}
                    width={64}
                  />
                )}
                <select
                  value={wm.capMode || "instances"}
                  onChange={(e) => set("capMode", e.target.value)}
                  style={{
                    background: "var(--bg-surface)",
                    border: "1px solid var(--border-accent)",
                    borderRadius: 3,
                    padding: "6px 8px",
                    color: "var(--text-primary)",
                    fontFamily: "inherit",
                    fontSize: 11,
                    outline: "none",
                    cursor: "pointer",
                  }}
                >
                  <option value="budget">USD/h cap</option>
                  <option value="instances">instance cap</option>
                </select>
              </div>
            </div>
            <div>
              <Label help={"- Installed inside the container instance\n- opengris-scaler must be included"}>requirements.txt</Label>
              <textarea
                value={wm.requirements || ""}
                onChange={(e) => set("requirements", e.target.value)}
                spellCheck={false}
                style={{
                  width: "100%",
                  background: "var(--bg-surface)",
                  border: "1px solid var(--border-accent)",
                  borderRadius: 3,
                  padding: "7px 10px",
                  color: "var(--text-primary)",
                  fontFamily: "inherit",
                  fontSize: 11,
                  outline: "none",
                  resize: "vertical",
                  minHeight: 72,
                  lineHeight: 1.6,
                }}
              />
            </div>
            <div
              style={{
                padding: "8px 10px",
                background: "rgba(0,255,136,0.04)",
                border: "1px solid var(--border-success)",
                borderRadius: 3,
                display: "flex",
                flexDirection: "column",
                gap: 4,
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
                  {wm.ociOcpus || 4} OCPU × ${ociPricing.ocpuPrice.toFixed(2)}/h
                </span>
                <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                  ${(ociPricing.ocpuPrice * (wm.ociOcpus || 4)).toFixed(2)}/h
                </span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
                  {wm.ociMemoryGb || 8} GB × ${ociPricing.memPrice.toFixed(3)}/h
                </span>
                <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                  ${(ociPricing.memPrice * (wm.ociMemoryGb || 8)).toFixed(2)}/h
                </span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", borderTop: "1px solid var(--border-success)", paddingTop: 4, marginTop: 2 }}>
                <span style={{ fontSize: 10, color: "var(--text-muted)" }}>Total ({derivedCount} instance{derivedCount !== 1 ? "s" : ""})</span>
                <span style={{ fontSize: 13, fontWeight: 600, color: "var(--text-success)" }}>
                  USD {(derivedCount * ociCostPerHr).toFixed(2)}/h
                </span>
              </div>
            </div>
          </>
        );
      })()}

      {/* oci_hpc */}
      {wm.type === "oci_hpc" && (
        <>
          <div>
            <Label help="OCI Compartment OCID where container instances are launched.">Compartment ID</Label>
            <input
              value={wm.ociCompartmentId || ""}
              onChange={(e) => set("ociCompartmentId", e.target.value)}
              style={inp}
              placeholder="ocid1.compartment.oc1..aaa..."
            />
          </div>
          <div>
            <Label help="OCI Availability Domain (e.g. AD-1 or Uocm:PHX-AD-1).">Availability Domain</Label>
            <input
              value={wm.ociAvailabilityDomain || ""}
              onChange={(e) => set("ociAvailabilityDomain", e.target.value)}
              style={inp}
              placeholder="AD-1"
            />
          </div>
          <div>
            <Label help="OCI Subnet OCID for container instance network interfaces.">Subnet ID</Label>
            <input
              value={wm.ociSubnetId || ""}
              onChange={(e) => set("ociSubnetId", e.target.value)}
              style={inp}
              placeholder="ocid1.subnet.oc1..aaa..."
            />
          </div>
          <div>
            <Label help={"The OCI Container Registry (OCIR) image URI for worker containers. In the OCI Console, go to Developer Services > Container Registry, select your repository, and copy the full path shown (format: <region>.ocir.io/<namespace>/<repo>:<tag>)."}>Container Image</Label>
            <input
              value={wm.ociContainerImage || ""}
              onChange={(e) => set("ociContainerImage", e.target.value)}
              style={inp}
              placeholder="us-ashburn-1.ocir.io/myns/scaler:latest"
            />
          </div>
          <div>
            <Label help={"The OCI region where your resources are located. To confirm the identifier in the OCI Console, click the region selector at the top of the page, then Manage Regions — identifiers are shown in the Region Identifier column."}>Region</Label>
            <OciRegionSelect
              value={wm.ociRegion || ""}
              onChange={(v) => set("ociRegion", v)}
            />
          </div>
          <div>
            <Label help={"Your tenancy's Object Storage namespace (a short unique string). In the OCI Console, go to Storage > Object Storage & Archive Storage > Buckets — the namespace is shown at the top of the page, or under Governance & Administration > Tenancy Details."}>Object Storage Namespace</Label>
            <input
              value={wm.ociObjectStorageNamespace || ""}
              onChange={(e) => set("ociObjectStorageNamespace", e.target.value)}
              style={inp}
              placeholder="mytenancy"
            />
          </div>
          <div>
            <Label help={"The OCI Object Storage bucket used to pass task inputs and results between the scheduler and workers. Create one in the OCI Console under Storage > Object Storage & Archive Storage > Buckets > Create Bucket."}>Object Storage Bucket</Label>
            <input
              value={wm.ociObjectStorageBucket || ""}
              onChange={(e) => set("ociObjectStorageBucket", e.target.value)}
              style={inp}
              placeholder="scaler-tasks-bucket"
            />
          </div>
          <div>
            <Label help="Key prefix for task inputs and results in Object Storage.">Object Storage Prefix</Label>
            <input
              value={wm.ociObjectStoragePrefix || "scaler-tasks"}
              onChange={(e) => set("ociObjectStoragePrefix", e.target.value)}
              style={inp}
              placeholder="scaler-tasks"
            />
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label help="Number of OCPUs allocated to each job container instance. Check OCI Container Instances documentation for the valid OCPU/memory combinations for your chosen shape.">OCPUs per Job</Label>
              <NumericStepper
                value={wm.ociOcpus || 1}
                onChange={(v) => set("ociOcpus", v)}
                min={1}
                max={64}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label help="Memory in GB allocated to each job container instance. Must satisfy OCI's minimum memory-per-OCPU ratio for your shape.">Memory (GB)</Label>
              <NumericStepper
                value={wm.ociMemoryGb || 6}
                onChange={(v) => set("ociMemoryGb", v)}
                min={1}
                max={512}
              />
            </div>
          </div>
          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}>
              <Label help="Maximum number of concurrently running container instances.">Max Concurrent Jobs</Label>
              <NumericStepper
                value={wm.ociMaxConcurrentJobs || 100}
                onChange={(v) => set("ociMaxConcurrentJobs", v)}
                min={1}
                max={10000}
              />
            </div>
            <div style={{ flex: 1 }}>
              <Label help="Maximum time in minutes a job container instance may run before being forcibly terminated by OCI.">Timeout (min)</Label>
              <NumericStepper
                value={wm.ociJobTimeoutMinutes || 60}
                onChange={(v) => set("ociJobTimeoutMinutes", v)}
                min={1}
                max={1440}
              />
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/* ── CopyBtn ── */
function copyText(text, onSuccess) {
  if (navigator.clipboard) {
    navigator.clipboard.writeText(text).then(onSuccess);
  } else {
    const el = document.createElement("textarea");
    el.value = text;
    el.style.position = "fixed";
    el.style.opacity = "0";
    document.body.appendChild(el);
    el.select();
    document.execCommand("copy");
    document.body.removeChild(el);
    onSuccess();
  }
}

function CopyBtn({ value }) {
  const [copied, setCopied] = useState(false);
  const [hov, setHov] = useState(false);
  return (
    <button
      onClick={() =>
        copyText(value, () => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        })
      }
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        background: hov && !copied ? "rgba(0,200,224,0.08)" : "none",
        border:
          "1px solid " +
          (copied
            ? "var(--border-success)"
            : hov
              ? "var(--border-strong)"
              : "var(--border-accent)"),
        borderRadius: 3,
        color: copied
          ? "var(--text-success)"
          : hov
            ? "var(--text-accent)"
            : "var(--text-muted)",
        fontFamily: "inherit",
        fontSize: 10,
        padding: "2px 7px",
        cursor: "pointer",
        letterSpacing: "0.06em",
        transition: "color 0.12s, border-color 0.12s, background 0.12s",
        flexShrink: 0,
      }}
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

/* ── DeploymentCard ── */
function DeploymentCard({ state, onDownload, keyMaterial, isRunning }) {
  const rows = [
    { label: "Scheduler", value: state.scheduler_address },
    { label: "Object storage", value: state.object_storage_address },
    { label: "Monitor", value: state.monitor_address },
    { label: "Worker Monitor", value: state.worker_monitor_address, href: state.worker_monitor_address },
    {
      label: "SSH",
      value: state.public_ip
        ? "chmod 400 " +
          state.key_file +
          " &&\nssh -i " +
          state.key_file +
          " ec2-user@" +
          state.public_ip
        : null,
      code: true,
    },
    { label: "Instance", value: state.instance_id },
  ];
  return (
    <div
      style={{
        background: "rgba(0,255,136,0.03)",
        border: "1px solid var(--border-success)",
        borderRadius: 4,
        padding: "20px 24px",
        display: "flex",
        flexDirection: "column",
        gap: 16,
        animation: "fadeSlideIn 0.3s ease",
      }}
    >
      <div style={{ display: "flex", alignItems: "center" }}>
        <div
          style={{
            fontSize: 11,
            color: "var(--text-success)",
            fontWeight: 600,
          }}
        >
          Active Deployment
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {rows.map(({ label, value, href, code }) => (
          <div
            key={label}
            style={{
              display: "flex",
              alignItems: code ? "flex-start" : "baseline",
              gap: 10,
            }}
          >
            <span
              style={{
                fontSize: 11,
                color: "var(--text-dim)",
                width: 120,
                flexShrink: 0,
                paddingTop: code ? 2 : 0,
              }}
            >
              {label}
            </span>
            <div
              style={{
                display: "flex",
                alignItems: code ? "flex-start" : "baseline",
                gap: 6,
                flex: 1,
                minWidth: 0,
              }}
            >
              {value ? (
                <>
                  {code ? (
                    <pre
                      style={{
                        fontSize: 11,
                        color: "var(--text-primary)",
                        fontFamily: "var(--font-mono)",
                        margin: 0,
                        whiteSpace: "pre",
                        overflowX: "auto",
                        flex: 1,
                        minWidth: 0,
                      }}
                    >
                      {value}
                    </pre>
                  ) : href ? (
                    <a
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{
                        fontSize: 12,
                        color: "var(--text-accent)",
                        fontWeight: 500,
                        overflowWrap: "anywhere",
                        whiteSpace: "pre-wrap",
                        fontFamily: "var(--font-mono)",
                        textDecoration: "none",
                        borderBottom: "1px solid var(--border-accent)",
                      }}
                    >
                      {value}
                    </a>
                  ) : (
                    <span
                      style={{
                        fontSize: 12,
                        color: "var(--text-primary)",
                        fontWeight: 500,
                        overflowWrap: "anywhere",
                        whiteSpace: "pre-wrap",
                        fontFamily: "var(--font-mono)",
                      }}
                    >
                      {value}
                    </span>
                  )}
                  <CopyBtn value={value} />
                </>
              ) : (
                <span
                  style={{
                    fontSize: 12,
                    color: "var(--text-dim)",
                    fontStyle: "italic",
                  }}
                >
                  pending…
                </span>
              )}
            </div>
          </div>
        ))}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span
            style={{
              fontSize: 11,
              color: "var(--text-dim)",
              width: 120,
              flexShrink: 0,
            }}
          >
            SSH Key
          </span>
          {keyMaterial ? (
            <button
              onClick={() =>
                downloadText(keyMaterial.name + ".pem", keyMaterial.mat)
              }
              style={{
                background: "none",
                border: "1px solid var(--border-accent)",
                borderRadius: 3,
                color: "var(--text-accent)",
                fontFamily: "inherit",
                fontSize: 10,
                padding: "3px 9px",
                cursor: "pointer",
                letterSpacing: "0.05em",
              }}
            >
              ↓ {keyMaterial.name}.pem
            </button>
          ) : isRunning ? (
            <span
              style={{
                fontSize: 12,
                color: "var(--text-dim)",
                fontStyle: "italic",
              }}
            >
              pending…
            </span>
          ) : (
            <span
              style={{
                fontSize: 11,
                color: "var(--text-dim)",
                lineHeight: 1.5,
              }}
            >
              not saved — download during provisioning
              <br />
              or retrieve from the AWS console
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

/* ── Python syntax highlighter (theme-aware, no external deps) ── */
const PY_KEYWORDS = new Set([
  "False",
  "None",
  "True",
  "and",
  "as",
  "assert",
  "async",
  "await",
  "break",
  "class",
  "continue",
  "def",
  "del",
  "elif",
  "else",
  "except",
  "finally",
  "for",
  "from",
  "global",
  "if",
  "import",
  "in",
  "is",
  "lambda",
  "nonlocal",
  "not",
  "or",
  "pass",
  "raise",
  "return",
  "try",
  "while",
  "with",
  "yield",
]);

function tokenizePython(code) {
  const tokens = [];
  const re =
    /(#[^\n]*)|("""[\s\S]*?"""|'''[\s\S]*?'''|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)(\s*\()?|(\s+|[^\w\s#"']+)/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    if (m[1]) tokens.push({ text: m[1], color: "var(--text-dim)" });
    else if (m[2]) tokens.push({ text: m[2], color: "var(--text-warning)" });
    else if (m[3]) tokens.push({ text: m[3], color: "var(--text-danger)" });
    else if (m[4]) {
      const word = m[4],
        call = m[5] || "";
      const color = PY_KEYWORDS.has(word)
        ? "var(--text-accent)"
        : call
          ? "var(--text-success)"
          : "var(--text-primary)";
      tokens.push({ text: word, color });
      if (call) tokens.push({ text: call, color: "var(--text-primary)" });
    } else {
      tokens.push({ text: m[6], color: "var(--text-primary)" });
    }
  }
  return tokens;
}

function PyCode({ code }) {
  const tokens = tokenizePython(code);
  return (
    <pre
      style={{
        margin: 0,
        padding: "14px 16px",
        background: "var(--bg-surface)",
        border: "1px solid var(--border-accent)",
        borderRadius: 3,
        fontSize: 11,
        fontFamily: "var(--font-mono)",
        whiteSpace: "pre",
        overflowX: "auto",
        lineHeight: 1.7,
      }}
    >
      {tokens.map((t, i) => (
        <span key={i} style={{ color: t.color }}>
          {t.text}
        </span>
      ))}
    </pre>
  );
}

/* ── GettingStartedCard ── */
function GettingStartedCard({ schedulerAddress, ready }) {
  const addr = schedulerAddress || "tcp://<scheduler-address>:2345";
  const snippet = `from scaler import Client

with Client(address="${addr}") as client:
    result = client.submit(pow, 2, 10).result()
    print(result)  # 1024`;

  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--border-accent)",
        borderRadius: 4,
        padding: "16px 20px",
        display: "flex",
        flexDirection: "column",
        gap: 12,
      }}
    >
      <div
        style={{
          fontSize: 11,
          color: "var(--text-muted)",
          fontWeight: 600,
        }}
      >
        Getting Started
      </div>
      {ready ? (
        <>
          <div
            style={{ fontSize: 11, color: "var(--text-dim)", lineHeight: 1.5 }}
          >
            Connect a client to your deployment and submit tasks:
          </div>
          <div style={{ position: "relative" }}>
            <PyCode code={snippet} />
            <div style={{ position: "absolute", top: 8, right: 8 }}>
              <CopyBtn value={snippet} />
            </div>
          </div>
        </>
      ) : (
        <div
          style={{
            fontSize: 11,
            color: "var(--text-dim)",
            fontStyle: "italic",
          }}
        >
          Waiting for scheduler…
        </div>
      )}
    </div>
  );
}

/* ── Try it tab: Pyodide + Monaco + Jedi ── */

const PYODIDE_CDN = "https://cdn.jsdelivr.net/pyodide/v0.29.3/full/pyodide.js";
const MONACO_VS   = "https://cdn.jsdelivr.net/npm/monaco-editor@0.52.2/min/vs";

function loadMonacoOnce() {
  if (window._monacoReady) return window._monacoReady;
  window.MonacoEnvironment = { getWorkerUrl: () => "data:text/javascript;charset=utf-8," };
  window._monacoReady = new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = MONACO_VS + "/loader.js";
    s.onload = () => {
      require.config({ paths: { vs: MONACO_VS } });
      require(["vs/editor/editor.main"], resolve);
    };
    s.onerror = () => reject(new Error("Failed to load Monaco from CDN"));
    document.head.appendChild(s);
  });
  return window._monacoReady;
}

function registerJediCompletion(pyodide) {
  const KIND = {
    function:  monaco.languages.CompletionItemKind.Function,
    class:     monaco.languages.CompletionItemKind.Class,
    module:    monaco.languages.CompletionItemKind.Module,
    instance:  monaco.languages.CompletionItemKind.Variable,
    keyword:   monaco.languages.CompletionItemKind.Keyword,
    statement: monaco.languages.CompletionItemKind.Variable,
    param:     monaco.languages.CompletionItemKind.TypeParameter,
    path:      monaco.languages.CompletionItemKind.File,
  };
  return monaco.languages.registerCompletionItemProvider("python", {
    triggerCharacters: [".", "(", "'", '"'],
    provideCompletionItems: async (model, position) => {
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber:   position.lineNumber,
        startColumn:     word.startColumn,
        endColumn:       position.column,
      };
      try {
        pyodide.globals.set("_jedi_code", model.getValue());
        pyodide.globals.set("_jedi_line", position.lineNumber);
        pyodide.globals.set("_jedi_col",  position.column - 1);
        const raw = await pyodide.runPythonAsync(
          'import jedi as _j, json as _js\n' +
          '_cs = _j.Interpreter(_jedi_code, [globals()]).complete(_jedi_line, _jedi_col)\n' +
          '_js.dumps([{"name": c.name, "type": c.type} for c in _cs[:80]])'
        );
        return {
          suggestions: JSON.parse(raw).map(({ name, type }) => ({
            label: name,
            kind: KIND[type] ?? monaco.languages.CompletionItemKind.Text,
            insertText: name,
            range,
          })),
        };
      } catch {
        return { suggestions: [] };
      }
    },
  });
}

const AUTO_IMPORT_PY =
  "import ast as _ast\n" +
  "try:\n" +
  "    _tree = _ast.parse(_auto_import_src)\n" +
  "    _stmts = [n for n in _tree.body if isinstance(n, (_ast.Import, _ast.ImportFrom))]\n" +
  "    if _stmts:\n" +
  "        exec(compile(_ast.Module(_stmts, []), '<auto-imports>', 'exec'), globals())\n" +
  "except Exception:\n" +
  "    pass\n";

function runAutoImports(pyodide, src) {
  pyodide.globals.set("_auto_import_src", src);
  return pyodide.runPythonAsync(AUTO_IMPORT_PY).catch(() => {});
}

function registerJediHover(pyodide) {
  return monaco.languages.registerHoverProvider("python", {
    provideHover: async (model, position) => {
      const wordInfo = model.getWordAtPosition(position);
      if (!wordInfo) return null;

      let doc = "";
      let name = wordInfo.word;

      // Primary: look up the name in globals() via Python so we can do proper
      // fallback to __init__.__doc__ when the class-level __doc__ is empty.
      try {
        pyodide.globals.set("_hover_word", wordInfo.word);
        const raw = await pyodide.runPythonAsync(
          'import json as _js, inspect as _ins\n' +
          '_obj = globals().get(_hover_word)\n' +
          'if _obj is not None:\n' +
          '    _doc = (_obj.__doc__ or\n' +
          '            getattr(getattr(_obj, "__init__", None), "__doc__", None) or "")\n' +
          '    _doc = _ins.cleandoc(_doc) if _doc else ""\n' +
          '    _name = getattr(_obj, "__qualname__", None) or getattr(_obj, "__name__", None) or _hover_word\n' +
          'else:\n' +
          '    _doc = ""\n' +
          '    _name = ""\n' +
          '_js.dumps({"name": _name, "doc": _doc})'
        );
        const item = JSON.parse(raw);
        if (item?.doc) { doc = item.doc; name = item.name || name; }
      } catch {}

      // Fallback: Jedi for local variables and other names not yet in globals.
      if (!doc) {
        try {
          pyodide.globals.set("_jedi_code", model.getValue());
          pyodide.globals.set("_jedi_line", position.lineNumber);
          pyodide.globals.set("_jedi_col",  position.column - 1);
          const raw = await pyodide.runPythonAsync(
            'import json as _js\n' +
            'try:\n' +
            '    import jedi as _j\n' +
            '    _hs = _j.Interpreter(_jedi_code, [globals()]).help(_jedi_line, _jedi_col)\n' +
            '    _h = _hs[0] if _hs else None\n' +
            '    _r = {"name": _h.full_name or _h.name, "doc": _h.docstring() or ""} if _h else {"name": "", "doc": ""}\n' +
            'except Exception:\n' +
            '    _r = {"name": "", "doc": ""}\n' +
            '_js.dumps(_r)'
          );
          const item = JSON.parse(raw);
          if (item?.doc) { doc = item.doc; name = item.name || name; }
        } catch {}
      }

      if (!doc) return null;

      return {
        range: {
          startLineNumber: position.lineNumber, endLineNumber: position.lineNumber,
          startColumn: wordInfo.startColumn, endColumn: wordInfo.endColumn,
        },
        contents: [
          { value: "```python\n" + name + "\n```" },
          { value: doc },
        ],
      };
    },
  });
}

function TryItTab({ isActive, theme, schedulerAddress }) {
  const [pyStatus, setPyStatus]   = useState("idle"); // idle | loading | ready | error
  const [pyError, setPyError]     = useState("");
  const [noWheels, setNoWheels]   = useState(false);
  const [output, setOutput]       = useState([]);
  const [isRunning, setIsRunning] = useState(false);
  const [monacoReady, setMonacoReady] = useState(false);

  const editorContainerRef     = useRef(null);
  const editorRef              = useRef(null);
  const pyodideRef             = useRef(null);
  const completionDisposable   = useRef(null);
  const hoverDisposable        = useRef(null);
  const hasInitEditor          = useRef(false);
  const isRunningRef           = useRef(false);
  const importTimerRef         = useRef(null);
  const outputCallbackRef      = useRef(null);
  const interruptBufferRef     = useRef(null);
  const cancelledRef           = useRef(false);

  const defaultCode = [
    "from scaler import Client",
    "",
    "# This is the address of the scheduler you launched",
    `SCHEDULER_ADDRESS = "${schedulerAddress}"`,
    "",
    "with Client(address=SCHEDULER_ADDRESS) as client:",
    "    futures = [client.submit(pow, 2, n) for n in range(8)]",
    "    results = [f.result() for f in futures]",
    "",
    "print(results)",
    "",
  ].join("\n");

  const runCode = useCallback(async () => {
    if (!editorRef.current || !pyodideRef.current || isRunningRef.current) return;
    isRunningRef.current = true;
    setIsRunning(true);
    setOutput([]);

    const pyodide = pyodideRef.current;
    cancelledRef.current = false;
    let hasOutput = false;
    const appendErr = (text) => { hasOutput = true; setOutput((prev) => [...prev, { text, cls: "err" }]); };
    outputCallbackRef.current = (text, cls) => { hasOutput = true; setOutput((prev) => [...prev, { text, cls }]); };

    try {
      pyodide.globals.set("_editor_code", editorRef.current.getValue());
      await pyodide.runPythonAsync(
        "import sys as _s, logging as _log\n" +
        "_log_h = _log.StreamHandler(_s.stdout)\n" +
        "_log_h.setFormatter(_log.Formatter('%(levelname)s:%(name)s: %(message)s'))\n" +
        "_root = _log.getLogger()\n" +
        "_saved_level = _root.level\n" +
        "_root.addHandler(_log_h)\n" +
        "if _root.level == 0 or _root.level > _log.INFO:\n" +
        "    _root.setLevel(_log.INFO)\n" +
        "try:\n" +
        "    exec(compile(_editor_code, '<editor>', 'exec'))\n" +
        "finally:\n" +
        "    _root.removeHandler(_log_h)\n" +
        "    _root.setLevel(_saved_level)\n"
      );
      if (!hasOutput) setOutput([{ text: "(no output)\n", cls: "dim" }]);
    } catch (err) {
      if (!cancelledRef.current) appendErr(err.message || String(err));
    } finally {
      outputCallbackRef.current = null;
      if (interruptBufferRef.current) Atomics.store(interruptBufferRef.current, 0, 0);
      isRunningRef.current = false;
      setIsRunning(false);
    }
  }, []);

  const cancelRun = useCallback(() => {
    if (!isRunningRef.current) return;
    cancelledRef.current = true;
    outputCallbackRef.current = null;
    if (interruptBufferRef.current) Atomics.store(interruptBufferRef.current, 0, 2);
    setOutput((prev) => [...prev, { text: "Cancelled.\n", cls: "dim" }]);
  }, []);

  // Monaco init — once, on first activation
  useEffect(() => {
    if (!isActive || hasInitEditor.current) return;
    hasInitEditor.current = true;
    loadMonacoOnce().then(() => {
      if (!editorContainerRef.current) return;
      editorRef.current = monaco.editor.create(editorContainerRef.current, {
        value: defaultCode,
        language: "python",
        theme: theme === "light" ? "vs" : "vs-dark",
        minimap: { enabled: false },
        fontSize: 13,
        fontFamily: '"JetBrains Mono", ui-monospace, monospace',
        automaticLayout: true,
        scrollBeyondLastLine: false,
        padding: { top: 12, bottom: 12 },
      });
      // Remeasure after custom fonts load so cursor aligns with JetBrains Mono glyphs
      document.fonts.ready.then(() => monaco.editor.remeasureFonts());
      editorRef.current.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
        () => runCode()
      );
      editorRef.current.onDidChangeModelContent(() => {
        clearTimeout(importTimerRef.current);
        importTimerRef.current = setTimeout(() => {
          if (!pyodideRef.current) return;
          runAutoImports(pyodideRef.current, editorRef.current?.getValue() ?? "");
        }, 600);
      });
      setMonacoReady(true);
    });
  }, [isActive]);

  // Re-measure editor when tab becomes visible again (display:none collapses dimensions)
  useEffect(() => {
    if (isActive) editorRef.current?.layout();
  }, [isActive]);

  // Dispose editor and completion provider only on unmount
  useEffect(() => {
    return () => {
      completionDisposable.current?.dispose();
      hoverDisposable.current?.dispose();
      editorRef.current?.dispose();
    };
  }, []);

  // Sync Monaco theme when the app theme changes
  useEffect(() => {
    if (!monacoReady) return;
    monaco.editor.setTheme(theme === "light" ? "vs" : "vs-dark");
  }, [theme, monacoReady]);

  // Pyodide init — once per page load, guarded at window level so remounts reuse the same instance
  useEffect(() => {
    if (!isActive || pyodideRef.current) return;
    setPyStatus("loading");
    (async () => {
      try {
        if (!window._pyodideReady) {
          window._pyodideReady = (async () => {
            // Lazy-load the Pyodide bootstrap script (~10 MB, deferred until tab open)
            await new Promise((resolve, reject) => {
              if (window.loadPyodide) { resolve(); return; }
              const s = document.createElement("script");
              s.src = PYODIDE_CDN;
              s.onload = resolve;
              s.onerror = () => reject(new Error("Failed to load Pyodide from CDN"));
              document.head.appendChild(s);
            });

            const pyodide = await window.loadPyodide();

            // Try fetching the wheel manifest written by generate_jupyterlite_config.py
            let manifest = null;
            try {
              const resp = await fetch("../_static/wasm/launchpad_wheels.json");
              if (resp.ok) manifest = await resp.json();
            } catch {}

            if (manifest) {
              await pyodide.loadPackage(["micropip"]);
              const base = new URL("../_static/wasm/", window.location.href).href;
              pyodide.globals.set(
                "_wheel_urls",
                pyodide.toPy(manifest.local_wheels.map((f) => base + f))
              );
              await pyodide.runPythonAsync(
                "import micropip\n" +
                "await micropip.install(list(_wheel_urls) + ['jedi'])"
              );
            } else {
              // No local wheels: plain Python REPL only, scaler unavailable
              pyodide._noWheels = true;
              await pyodide.loadPackage(["micropip"]);
              await pyodide.runPythonAsync(
                "import micropip; await micropip.install('jedi')"
              );
            }

            return pyodide;
          })();
        }

        const pyodide = await window._pyodideReady;
        pyodideRef.current = pyodide;
        if (pyodide._noWheels) setNoWheels(true);
        pyodide.setStdout({ batched: (text) => outputCallbackRef.current?.(text, "info") });
        pyodide.setStderr({ batched: (text) => outputCallbackRef.current?.(text, "err")  });
        try {
          const buf = new Int32Array(new SharedArrayBuffer(4));
          pyodide.setInterruptBuffer(buf);
          interruptBufferRef.current = buf;
        } catch {}
        setPyStatus("ready");
      } catch (err) {
        console.error("Pyodide init failed:", err);
        setPyError(String(err));
        setPyStatus("error");
      }
    })();
  }, [isActive]);

  // Register Jedi completions and hover once both Monaco and Pyodide are ready,
  // and eagerly run the editor's import statements so autocomplete works immediately.
  useEffect(() => {
    if (!monacoReady || pyStatus !== "ready" || !pyodideRef.current) return;
    completionDisposable.current?.dispose();
    hoverDisposable.current?.dispose();
    completionDisposable.current = registerJediCompletion(pyodideRef.current);
    hoverDisposable.current      = registerJediHover(pyodideRef.current);
    runAutoImports(pyodideRef.current, editorRef.current?.getValue() ?? "");
  }, [monacoReady, pyStatus]);

  const canRun = monacoReady && pyStatus === "ready" && !isRunning;

  const statusNode = (() => {
    if (pyStatus === "idle") return null;
    if (pyStatus === "loading")
      return (
        <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
          <span style={{ animation: "blink 1s step-start infinite", marginRight: 5 }}>●</span>
          Loading Pyodide…
        </span>
      );
    if (pyStatus === "error")
      return (
        <span style={{ fontSize: 11, color: "var(--text-danger)" }}>
          ● Pyodide failed
        </span>
      );
    return (
      <span style={{ fontSize: 11, color: "var(--text-success)" }}>
        ● ready
        {noWheels && (
          <span style={{ color: "var(--text-warning)", marginLeft: 6 }}>
            (wasm wheels not built — import scaler will fail)
          </span>
        )}
      </span>
    );
  })();

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      {/* Toolbar */}
      <div style={{
        display: "flex", alignItems: "center", gap: 10,
        padding: "7px 16px",
        background: "var(--bg-panel)",
        borderBottom: "1px solid var(--border-accent)",
        flexShrink: 0,
      }}>
        <button
          onClick={isRunning ? cancelRun : (canRun ? runCode : undefined)}
          disabled={!canRun && !isRunning}
          style={{
            padding: "5px 14px",
            background: isRunning
              ? "transparent"
              : canRun
              ? "linear-gradient(135deg, oklch(0.38 0.16 155) 0%, oklch(0.32 0.14 200) 100%)"
              : "var(--bg-surface)",
            border: "1px solid " + (isRunning ? "var(--text-danger)" : canRun ? "oklch(0.55 0.16 155)" : "var(--border-accent)"),
            borderRadius: 3,
            color: isRunning ? "var(--text-danger)" : canRun ? "oklch(0.92 0.1 155)" : "var(--text-dim)",
            fontFamily: "inherit", fontSize: 11, fontWeight: 700,
            cursor: (canRun || isRunning) ? "pointer" : "default",
            flexShrink: 0,
          }}
        >
          {isRunning ? "✕ Cancel" : "▶ Run  Ctrl+Enter"}
        </button>
        <button
          onClick={() => setOutput([])}
          style={{
            padding: "5px 10px",
            background: "transparent",
            border: "1px solid var(--border-accent)",
            borderRadius: 3,
            color: "var(--text-muted)",
            fontFamily: "inherit", fontSize: 11, cursor: "pointer",
            flexShrink: 0,
          }}
        >
          Clear output
        </button>
        <div style={{ flex: 1 }} />
        {statusNode}
      </div>

      {/* Editor | Output split */}
      <div style={{ display: "flex", flex: 1, minHeight: 0 }}>
        <div
          ref={editorContainerRef}
          style={{ flex: "0 0 60%", minHeight: 0, position: "relative" }}
        >
          {!monacoReady && (
            <div style={{
              position: "absolute", inset: 0,
              display: "flex", alignItems: "center", justifyContent: "center",
              color: "var(--text-dim)", fontSize: 12,
              background: "var(--bg-page)",
            }}>
              Loading editor…
            </div>
          )}
        </div>

        {/* Output panel */}
        <div style={{
          flex: "0 0 40%", minHeight: 0,
          borderLeft: "1px solid var(--border-accent)",
          display: "flex", flexDirection: "column",
        }}>
          <div style={{
            padding: "5px 12px",
            fontSize: 10,
            color: "var(--text-dim)",
            borderBottom: "1px solid var(--border-accent)",
            background: "var(--bg-panel)",
            flexShrink: 0,
            letterSpacing: "0.06em",
          }}>
            OUTPUT
          </div>
          <div style={{
            flex: 1, minHeight: 0, overflowY: "auto",
            background: "var(--term-bg)",
            padding: "10px 14px",
            fontFamily: "var(--font-mono)",
            fontSize: 12,
            lineHeight: 1.7,
          }}>
            {output.length === 0 && !isRunning && pyStatus === "ready" && (
              <span style={{ color: "var(--text-dim)", fontStyle: "italic" }}>
                Press ▶ Run or Ctrl+Enter to execute
              </span>
            )}
            {output.map((line, i) => (
              <div key={i} style={{
                color: line.cls === "err"  ? "var(--text-danger)"
                     : line.cls === "warn" ? "var(--text-warning)"
                     : line.cls === "dim"  ? "var(--text-dim)"
                     : "var(--text-secondary)",
                whiteSpace: "pre-wrap",
                wordBreak: "break-all",
              }}>
                {line.text}
              </div>
            ))}
            {isRunning && (
              <span style={{ color: "var(--text-success)", animation: "blink 1s step-end infinite" }}>▌</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── TopNav ── */
function TopNav({
  activeTab,
  setActiveTab,
  theme,
  setTheme,
  showPostLaunch,
  schedulerReady,
  launchControl,
}) {
  const tabs = [
    { id: "config", label: "Config" },
    { id: "deployment", label: "Deployment", postLaunch: true },
    { id: "logs", label: "Scheduler Logs", requiresScheduler: true },
    // { id: "worker-monitor", label: "Worker Monitor", postLaunch: true },
    { id: "worker-monitor", label: "Worker Monitor", requiresScheduler: true },
    { id: "try-it", label: "Try it", requiresScheduler: true },
  ];
  return (
    <div
      style={{
        padding: "0 28px",
        borderBottom: "1px solid var(--border-accent)",
        background: "var(--bg-panel)",
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
      }}
    >
      <img
        src="https://raw.githubusercontent.com/finos/branding/master/project-logos/active-project-logos/OpenGRIS/Scaler/2025_OpenGRIS_Scaler.svg"
        alt="OpenGRIS Scaler"
        style={{ height: 34, marginRight: 28, flexShrink: 0 }}
      />
      <div style={{ display: "flex", flex: 1 }}>
        {tabs.map((t) => {
          const disabled = (t.postLaunch && !showPostLaunch) || (t.requiresScheduler && !schedulerReady);
          return t.isLink ? (
            <a
              key={t.id}
              href={disabled ? undefined : t.href}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                padding: "14px 18px",
                background: "transparent",
                border: "none",
                borderBottom: "2px solid transparent",
                color:
                  disabled || !t.href ? "var(--text-dim)" : "var(--text-muted)",
                fontFamily: "inherit",
                fontSize: 12,
                cursor: disabled || !t.href ? "default" : "pointer",
                textDecoration: "none",
                display: "inline-flex",
                alignItems: "center",
                pointerEvents: disabled || !t.href ? "none" : undefined,
                opacity: disabled ? 0.4 : 1,
              }}
            >
              {t.label} ↗
            </a>
          ) : (
            <button
              key={t.id}
              onClick={() => !disabled && setActiveTab(t.id)}
              style={{
                padding: "14px 18px",
                background: "transparent",
                border: "none",
                borderBottom:
                  activeTab === t.id
                    ? "2px solid var(--tab-active)"
                    : "2px solid transparent",
                color: disabled
                  ? "var(--text-dim)"
                  : activeTab === t.id
                    ? "var(--text-accent)"
                    : "var(--text-muted)",
                fontFamily: "inherit",
                fontSize: 12,
                cursor: disabled ? "default" : "pointer",
                opacity: disabled ? 0.4 : 1,
              }}
            >
              {t.label}
            </button>
          );
        })}
      </div>
      {launchControl && <div style={{ marginRight: 16 }}>{launchControl}</div>}
      {IS_DEV && (
        <div
          title="DEV mode: credentials are persisted to sessionStorage for convenience. Note that browsers may write sessionStorage to disk for session-restore/crash-recovery, so plaintext secrets can outlive a tab close."
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.1em",
            color: "var(--text-warning)",
            background: "rgba(255,202,22,0.08)",
            border: "1px solid rgba(255,202,22,0.25)",
            borderRadius: 3,
            padding: "2px 7px",
            marginRight: 12,
            flexShrink: 0,
            cursor: "help",
          }}
        >
          DEV
        </div>
      )}
      <label
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          fontSize: 11,
          color: "var(--text-muted)",
        }}
      >
        Theme
        <select
          value={theme}
          onChange={(e) => setTheme(e.target.value)}
          style={{
            background: "var(--bg-surface)",
            border: "1px solid var(--border-accent)",
            borderRadius: 3,
            color: "var(--text-secondary)",
            fontFamily: "inherit",
            fontSize: 10,
            padding: "4px 8px",
            cursor: "pointer",
            outline: "none",
          }}
        >
          <option value="dark">Dark</option>
          <option value="light">Light</option>
          <option value="zenburn">Zenburn</option>
        </select>
      </label>
    </div>
  );
}

/* ── App ── */
function App() {
  const [region, setRegion] = useState("us-east-1");
  const [accessKeyId, setAKI] = useState(() => IS_DEV ? (sessionStorage.getItem('launchpad-dev-aki') || '') : '');
  const [secretKey, setSK] = useState(() => IS_DEV ? (sessionStorage.getItem('launchpad-dev-sk') || '') : '');
  const [credTab, setCredTab] = useState("aws");
  const [ociUserId, setOciUserId] = useState(() => IS_DEV ? (sessionStorage.getItem('launchpad-dev-oci-uid') || '') : '');
  const [ociTenancyId, setOciTenancyId] = useState(() => IS_DEV ? (sessionStorage.getItem('launchpad-dev-oci-tid') || '') : '');
  const [ociFingerprint, setOciFingerprint] = useState(() => IS_DEV ? (sessionStorage.getItem('launchpad-dev-oci-fp') || '') : '');
  const [ociPrivateKey, setOciPrivateKey] = useState(() => IS_DEV ? (sessionStorage.getItem('launchpad-dev-oci-pk') || '') : '');
  const [transport, setTransport] = useState("ws");
  const [networkBackend, setNetBack] = useState("ymq");
  const [pythonVersion, setPyVer] = useState("3.13");
  const [policy, setPolicy] = useState("simple");
  const [schedulerRequirements, setSchedulerReqs] = useState(
    "opengris-scaler[all]",
  );
  const [schedulerType, setSchedulerType] = useState("c5.xlarge");
  const [schedulerPort, setSchedPort] = useState(6788);
  const [objectStoragePort, setObjPort] = useState(6789);
  const [activeTab, setActiveTab] = useState("config");
  const [theme, setTheme] = useState(
    () =>
      localStorage.getItem("launchpad-theme") ||
      (window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"),
  );

  const wmCounterRef = useRef(1);
  const uidCounterRef = useRef(1);
  const loadConfigInputRef = useRef(null);
  const [workerManagers, setWorkerManagers] = useState([
    {
      _uid: 1,
      id: "wm-1",
      type: "orb_aws_ec2",
      instanceType: "t3.medium",
      capMode: "instances",
      instanceCap: 4,
      budgetCap: 10,
      requirements: "opengris-scaler[all]",
    },
  ]);
  const [selectedWmId, setSelectedWmId] = useState("wm-1");
  const [draggedWmId, setDraggedWmId] = useState(null);
  const [dragOverWmId, setDragOverWmId] = useState(null);

  const [phase, setPhase] = useState(() => {
    try {
      return localStorage.getItem("scaler_state") ? "ready" : "idle";
    } catch {
      return "idle";
    }
  });
  const [log, setLog] = useState(() => {
    try {
      const s = localStorage.getItem("scaler_log");
      return s ? JSON.parse(s) : [];
    } catch {
      return [];
    }
  });
  const [provState, setProvState] = useState(() => {
    try {
      const s = localStorage.getItem("scaler_state");
      return s ? JSON.parse(s) : null;
    } catch {
      return null;
    }
  });
  const [keyMaterial, setKeyMaterial] = useState(null);
  const [pausedOp, setPausedOp] = useState(null); // "deploy" | "teardown"
  const abortRef = useRef(null);
  const partialRef = useRef(null); // latest partial state, readable synchronously in catch blocks

  const [workerMonitorReady, setWorkerMonitorReady] = useState(false);
  const [workerMonitorElapsed, setWorkerMonitorElapsed] = useState(0);

  useEffect(() => {
    const addr = provState?.worker_monitor_address;
    if (!addr) {
      setWorkerMonitorReady(false);
      setWorkerMonitorElapsed(0);
      return;
    }
    setWorkerMonitorReady(false);
    setWorkerMonitorElapsed(0);
    let cancelled = false;
    const start = Date.now();
    const ticker = setInterval(() => {
      if (!cancelled) setWorkerMonitorElapsed(Math.floor((Date.now() - start) / 1000));
    }, 1000);
    const poll = async () => {
      if (cancelled) return;
      const ctrl = new AbortController();
      const timeout = setTimeout(() => ctrl.abort(), 4000);
      try {
        await fetch(addr, {
          mode: "no-cors",
          cache: "no-store",
          signal: ctrl.signal,
        });
        clearTimeout(timeout);
        if (!cancelled) {
          setWorkerMonitorReady(true);
          clearInterval(ticker);
        }
      } catch {
        clearTimeout(timeout);
        if (!cancelled) setTimeout(poll, 5000);
      }
    };
    poll();
    return () => {
      cancelled = true;
      clearInterval(ticker);
    };
  }, [provState?.worker_monitor_address]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("launchpad-theme", theme);
  }, [theme]);

  // DEV convenience: persist credentials across refreshes. sessionStorage is cleared on tab
  // close but browsers may flush it to disk for crash-recovery, so secrets can outlive a reload.
  useEffect(() => {
    if (!IS_DEV) return;
    sessionStorage.setItem('launchpad-dev-aki', accessKeyId);
    sessionStorage.setItem('launchpad-dev-sk', secretKey);
    sessionStorage.setItem('launchpad-dev-oci-uid', ociUserId);
    sessionStorage.setItem('launchpad-dev-oci-tid', ociTenancyId);
    sessionStorage.setItem('launchpad-dev-oci-fp', ociFingerprint);
    sessionStorage.setItem('launchpad-dev-oci-pk', ociPrivateKey);
  }, [accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey]);

  useEffect(() => {
    try {
      localStorage.setItem("scaler_log", JSON.stringify(log));
    } catch (_) {}
  }, [log]);

  useEffect(() => {
    if (phase === "provisioning") setActiveTab("deployment");
  }, [phase]);

  const addLog = useCallback((text, cls) => {
    setLog((prev) => [...prev, { text, cls: cls || "info" }]);
  }, []);
  const savePartial = useCallback((partial) => {
    partialRef.current = partial;
    setProvState(partial);
    try {
      localStorage.setItem("scaler_state", JSON.stringify(partial));
    } catch (_) {}
  }, []);

  const allInstances = window.SCALER_INSTANCES || [];
  const schedulerInst = allInstances.find((i) => i.type === schedulerType) || {
    price: 0.17,
  };
  const wmCosts = workerManagers.map((wm) => {
    if (wm.type === "orb_aws_ec2") {
      const inst = allInstances.find((i) => i.type === wm.instanceType) || { price: 0 };
      const count =
        wm.capMode === "instances"
          ? Math.max(0, wm.instanceCap || 0)
          : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
      return count * inst.price;
    }
    if (wm.type === "oci_raw") {
      const shape = wm.ociShape || "CI.Standard.A1.Flex";
      const pricing = OCI_SHAPE_PRICING[shape] || OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
      const costPerInstance = pricing.ocpuPrice * (wm.ociOcpus || 4) + pricing.memPrice * (wm.ociMemoryGb || 8);
      const count = wm.capMode === "instances"
        ? Math.max(0, wm.instanceCap || 0)
        : Math.max(0, Math.floor((wm.budgetCap || 0) / (costPerInstance || 1)));
      return count * costPerInstance;
    }
    return 0;
  });
  const totalCostPerHr =
    schedulerInst.price + wmCosts.reduce((a, b) => a + b, 0);

  const addWorkerManager = useCallback(() => {
    setWorkerManagers((prev) => {
      const existingIds = new Set(prev.map((w) => w.id));
      do { wmCounterRef.current += 1; } while (existingIds.has("wm-" + wmCounterRef.current));
      const newId = "wm-" + wmCounterRef.current;
      setSelectedWmId(newId);
      return [
        ...prev,
        {
          _uid: ++uidCounterRef.current,
          id: newId,
          type: "orb_aws_ec2",
          instanceType: "t3.medium",
          capMode: "instances",
          instanceCap: 4,
          budgetCap: 10,
          requirements: "opengris-scaler[all]",
        },
      ];
    });
  }, []);
  const removeWorkerManager = useCallback((id) => {
    setWorkerManagers((prev) => {
      const next = prev.filter((wm) => wm.id !== id);
      setSelectedWmId((s) => (s === id ? next[0]?.id || "" : s));
      return next;
    });
  }, []);
  const updateWorkerManager = useCallback(
    (id, updated) =>
      setWorkerManagers((prev) =>
        prev.map((wm) => (wm.id === id ? updated : wm)),
      ),
    [],
  );
  const hasCredentials =
    accessKeyId.trim().length > 0 && secretKey.trim().length > 0;

  const monitorPort = schedulerPort + 2;
  const WORKER_MONITOR_PORT = 50001;
  const portConflicts = [];
  if (schedulerPort === objectStoragePort)
    portConflicts.push("Scheduler port and object storage port must differ.");
  if (objectStoragePort === monitorPort)
    portConflicts.push(
      `Object storage port conflicts with the monitor port (scheduler + 2 = ${monitorPort}).`,
    );
  if (schedulerPort === WORKER_MONITOR_PORT)
    portConflicts.push(
      `Scheduler port conflicts with the Worker Monitor port (${WORKER_MONITOR_PORT}).`,
    );
  if (objectStoragePort === WORKER_MONITOR_PORT)
    portConflicts.push(
      `Object storage port conflicts with the Worker Monitor port (${WORKER_MONITOR_PORT}).`,
    );
  if (monitorPort === WORKER_MONITOR_PORT)
    portConflicts.push(
      `Monitor port (scheduler + 2 = ${monitorPort}) conflicts with the Worker Monitor port (${WORKER_MONITOR_PORT}).`,
    );

  const hasOciWm = workerManagers.some((wm) => wm.type === "oci_raw");
  const hasOciCredentials =
    ociUserId.trim().length > 0 &&
    ociTenancyId.trim().length > 0 &&
    ociFingerprint.trim().length > 0 &&
    ociPrivateKey.trim().length > 0;
  const ociWmErrors = workerManagers
    .filter((wm) => wm.type === "oci_raw")
    .flatMap((wm) => {
      const errs = [];
      if (!wm.ociCompartmentId?.trim()) errs.push(`OCI worker manager '${wm.id}' requires a Compartment ID`);
      if (!wm.ociAvailabilityDomain?.trim()) errs.push(`OCI worker manager '${wm.id}' requires an Availability Domain`);
      if (!wm.ociSubnetId?.trim()) errs.push(`OCI worker manager '${wm.id}' requires a Subnet ID`);
      return errs;
    });

  const checks = [
    {
      key: "aki",
      label: "AWS Access Key ID is required",
      ok: accessKeyId.trim().length > 0,
    },
    {
      key: "sk",
      label: "AWS Secret Access Key is required",
      ok: secretKey.trim().length > 0,
    },
    {
      key: "wm",
      label: "At least one worker manager must be configured",
      ok: workerManagers.length > 0,
    },
    {
      key: "wm_ids",
      label: "Worker manager IDs must be unique",
      ok: new Set(workerManagers.map((w) => w.id)).size === workerManagers.length,
    },
    {
      key: "ports",
      label: portConflicts.join(" "),
      ok: portConflicts.length === 0,
    },
    ...(hasOciWm
      ? [
          {
            key: "oci_creds",
            label: "An OCI worker manager is configured but OCI credentials have not been provided",
            ok: hasOciCredentials,
          },
          ...ociWmErrors.map((msg, i) => ({ key: `oci_wm_${i}`, label: msg, ok: false })),
        ]
      : []),
  ];
  const blocking = checks.filter((c) => !c.ok);
  const formReady = blocking.length === 0;
  const isRunning = phase === "provisioning" || phase === "destroying";
  const isPaused = phase === "paused";

  const handleLaunch = useCallback(async () => {
    const isResume = phase === "paused" && pausedOp === "deploy";
    if (!isResume && !formReady) return;
    setLog((prev) => (isResume ? [...prev, { text: "", cls: "dim" }] : []));
    if (!isResume) {
      try {
        localStorage.removeItem("scaler_log");
      } catch (_) {}
    }
    setPhase("provisioning");
    setPausedOp(null);
    const resumeState = isResume ? provState : null;
    const cfg = {
      region,
      nameSuffix: resumeState ? resumeState.name_suffix : randomSuffix(),
      instanceType: schedulerType,
      amiId: null,
      transport,
      networkBackend,
      schedulerPort,
      objectStoragePort,
      pythonVersion,
      policy,
      scalerPackage: schedulerRequirements,
      instanceProfileName: null,
      pollTimeout: 600,
      pollInterval: 15,
      debugDumpPath: null,
      workerManagers: workerManagers.map((wm) => ({
        ...wm,
        requirements: wm.requirements,
      })),
    };
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const state = await provision(
        cfg,
        { accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey },
        addLog,
        savePartial,
        (name, mat) => setKeyMaterial({ name, mat }),
        controller.signal,
        resumeState,
      );
      savePartial(state);
      setPhase("ready");
    } catch (err) {
      if (err.name === "RetryPausedError") {
        addLog(
          "\nDeployment paused — retries exhausted: " + err.message +
            "\nUse Resume to continue or switch to teardown.",
          "warn",
        );
        setPausedOp("deploy");
        setPhase("paused");
      } else if (err.name === "AbortError") {
        addLog(
          "\nAborted. Any resources created so far are saved — use Destroy to clean them up.",
          "warn",
        );
        setPhase("error");
      } else {
        const snapshot = partialRef.current;
        if (snapshot) {
          addLog("\nProvisioning failed: " + err.message, "err");
          addLog("Auto-tearing down created resources…", "warn");
          setPhase("destroying");
          const tdController = new AbortController();
          abortRef.current = tdController;
          try {
            await teardown(
              snapshot,
              { accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey },
              addLog,
              tdController.signal,
            );
            try {
              localStorage.removeItem("scaler_state");
              localStorage.removeItem("scaler_log");
            } catch (_) {}
            setProvState(null);
            partialRef.current = null;
            setKeyMaterial(null);
            setPhase("idle");
          } catch (tdErr) {
            if (tdErr.name === "RetryPausedError") {
              addLog(
                "\nAuto-teardown paused — retries exhausted. Use Resume Teardown.",
                "warn",
              );
              setPausedOp("teardown");
              setPhase("paused");
            } else {
              addLog("\nAuto-teardown failed: " + tdErr.message, "err");
              setPhase("error");
            }
          }
        } else {
          addLog("\nError: " + err.message, "err");
          setPhase("error");
        }
      }
    } finally {
      abortRef.current = null;
    }
  }, [
    phase,
    pausedOp,
    provState,
    formReady,
    region,
    schedulerType,
    transport,
    networkBackend,
    schedulerPort,
    objectStoragePort,
    pythonVersion,
    schedulerRequirements,
    workerManagers,
    accessKeyId,
    secretKey,
    ociUserId,
    ociTenancyId,
    ociFingerprint,
    ociPrivateKey,
    addLog,
    savePartial,
  ]);

  const handleAbort = useCallback(() => {
    if (abortRef.current) abortRef.current.abort();
  }, []);

  const handleDestroy = useCallback(async () => {
    if (!provState || !hasCredentials) return;
    const isResume = phase === "paused" && pausedOp === "teardown";
    if (!isResume) {
      if (
        !window.confirm(
          "Terminate all AWS resources in this deployment?\n\n" +
            "• EC2 instance: " +
            (provState.instance_id || "—") +
            "\n" +
            "• Security group: " +
            (provState.security_group_id || "—") +
            "\n" +
            "• Key pair: " +
            (provState.key_pair_name || "—") +
            "\n" +
            (provState.iam && provState.iam.created
              ? "• IAM role & profile\n"
              : "") +
            "\nThis cannot be undone.",
        )
      )
        return;
    }
    setPhase("destroying");
    setPausedOp(null);
    if (!isResume) setActiveTab("deployment");
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      await teardown(
        provState,
        { accessKeyId, secretKey, ociUserId, ociTenancyId, ociFingerprint, ociPrivateKey },
        addLog,
        controller.signal,
      );
      try {
        localStorage.removeItem("scaler_state");
        localStorage.removeItem("scaler_log");
      } catch (_) {}
      setProvState(null);
      setKeyMaterial(null);
      setPhase("idle");
    } catch (err) {
      if (err.name === "RetryPausedError") {
        addLog(
          "\nTeardown paused — retries exhausted: " + err.message +
            "\nUse Resume Teardown to retry.",
          "warn",
        );
        setPausedOp("teardown");
        setPhase("paused");
      } else if (err.name === "AbortError") {
        addLog(
          "\nTeardown aborted. Some resources may still exist — run Destroy again to retry.",
          "warn",
        );
        setPhase("ready");
      } else {
        addLog(
          "\nError during teardown: " + err.message + "\nFix the issue and run Destroy again to retry.",
          "err",
        );
        setPhase("ready");
      }
    } finally {
      abortRef.current = null;
    }
  }, [phase, pausedOp, provState, hasCredentials, accessKeyId, secretKey, addLog, setActiveTab]);

  const handleDownloadConfig = useCallback(() => {
    const cfg = {
      region,
      transport,
      networkBackend,
      schedulerPort,
      objectStoragePort,
      pythonVersion,
      policy,
      workerManagers: workerManagers.map((wm) => ({
        ...wm,
        requirements: wm.requirements,
      })),
    };
    downloadText("config.toml", buildConfigToml(cfg));
  }, [
    region,
    transport,
    networkBackend,
    schedulerPort,
    objectStoragePort,
    pythonVersion,
    policy,
    workerManagers,
  ]);

  const handleLoadConfig = useCallback((e) => {
    const file = e.target.files[0];
    if (!file) return;
    e.target.value = "";
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const toml = parseConfigToml(ev.target.result);
        const cfg = configFromToml(toml);
        if (cfg.transport) setTransport(cfg.transport);
        if (cfg.schedulerPort) setSchedPort(cfg.schedulerPort);
        if (cfg.objectStoragePort) setObjPort(cfg.objectStoragePort);
        if (cfg.pythonVersion) setPyVer(cfg.pythonVersion);
        if (cfg.region) setRegion(cfg.region);
        if (cfg.policy) setPolicy(cfg.policy);
        if (cfg.networkBackend) setNetBack(cfg.networkBackend);
        if (cfg.workerManagers && cfg.workerManagers.length) {
          const wms = cfg.workerManagers.map((wm) => ({ ...wm, _uid: ++uidCounterRef.current }));
          setWorkerManagers(wms);
          setSelectedWmId(wms[0].id);
        }
      } catch (err) {
        window.alert("Failed to load config.toml: " + err.message);
      }
    };
    reader.readAsText(file);
  }, []);

  const handleReset = useCallback(() => {
    setLog([]);
    setPhase("idle");
    setProvState(null);
    setKeyMaterial(null);
    try {
      localStorage.removeItem("scaler_state");
      localStorage.removeItem("scaler_log");
    } catch (_) {}
  }, []);

  const Label = ({ children, help }) => (
    <div
      style={{
        fontSize: 11,
        color: "var(--text-label)",
        marginBottom: 5,
        display: "flex",
        alignItems: "center",
        gap: 6,
      }}
    >
      <span>{children}</span>
      {help && <HelpTip text={help} />}
    </div>
  );
  const inp = {
    width: "100%",
    background: "var(--bg-surface)",
    border: "1px solid var(--border-accent)",
    borderRadius: 3,
    padding: "7px 10px",
    color: "var(--text-primary)",
    fontFamily: "inherit",
    fontSize: 12,
    outline: "none",
  };
  const TogglePair = ({ options, value, onSelect }) => (
    <div
      style={{
        display: "flex",
        borderRadius: 3,
        overflow: "hidden",
        border: "1px solid var(--border-accent)",
      }}
    >
      {options.map(([val, lbl, dis]) => (
        <button
          key={val}
          disabled={!!dis}
          onClick={() => !dis && onSelect(val)}
          style={{
            flex: 1,
            padding: "7px 0",
            fontFamily: "inherit",
            fontSize: 11,
            cursor: dis ? "not-allowed" : "pointer",
            border: "none",
            background: value === val ? "rgba(0,200,224,0.18)" : "transparent",
            color: dis
              ? "var(--text-dim)"
              : value === val
                ? "var(--text-accent)"
                : "var(--text-muted)",
            transition: "background 0.15s, color 0.15s",
          }}
        >
          {lbl}
        </button>
      ))}
    </div>
  );

  const _destroyBtnStyle = (disabled) => ({
    padding: "8px 20px",
    background: disabled
      ? "rgba(255,80,60,0.04)"
      : "linear-gradient(135deg, oklch(0.32 0.18 15) 0%, oklch(0.26 0.14 30) 100%)",
    border: "1px solid " + (disabled ? "var(--border-danger)" : "oklch(0.48 0.18 15)"),
    borderRadius: 4,
    color: disabled ? "var(--text-danger)" : "oklch(0.88 0.1 30)",
    fontFamily: "inherit",
    fontSize: 11,
    fontWeight: 700,
    cursor: disabled ? "default" : "pointer",
    transition: "all 0.2s",
    flexShrink: 0,
  });

  let launchControl;
  if (isPaused) {
    launchControl = (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div
          style={{
            padding: "6px 12px",
            background: "rgba(255,160,60,0.06)",
            border: "1px solid rgba(255,160,60,0.35)",
            borderRadius: 4,
            color: "var(--text-warning)",
            fontSize: 11,
          }}
        >
          {pausedOp === "teardown" ? "Teardown paused" : "Deploy paused"}
        </div>
        {pausedOp === "deploy" && (
          <button
            onClick={handleLaunch}
            style={{
              padding: "6px 14px",
              background: "linear-gradient(135deg, oklch(0.38 0.16 155) 0%, oklch(0.32 0.14 200) 100%)",
              border: "1px solid oklch(0.55 0.16 155)",
              borderRadius: 4,
              color: "oklch(0.92 0.1 155)",
              fontFamily: "inherit",
              fontSize: 11,
              fontWeight: 700,
              cursor: "pointer",
              flexShrink: 0,
            }}
          >
            Resume Deploy
          </button>
        )}
        <button
          onClick={handleDestroy}
          disabled={!hasCredentials}
          style={_destroyBtnStyle(!hasCredentials)}
        >
          {pausedOp === "teardown" ? "Resume Teardown" : "Switch to Teardown"}
          {!hasCredentials ? " (missing credentials)" : ""}
        </button>
      </div>
    );
  } else if (phase === "error" && provState) {
    launchControl = (
      <button
        onClick={handleDestroy}
        disabled={!hasCredentials}
        style={_destroyBtnStyle(!hasCredentials)}
      >
        Destroy Cluster{!hasCredentials ? " (missing credentials)" : ""}
      </button>
    );
  } else if (phase === "idle" || phase === "error") {
    const launchBtn = (
      <button
        onClick={formReady ? handleLaunch : undefined}
        disabled={!formReady}
        style={{
          padding: "8px 20px",
          background: !formReady
            ? "rgba(229,72,77,0.05)"
            : "linear-gradient(135deg, oklch(0.38 0.16 155) 0%, oklch(0.32 0.14 200) 100%)",
          border:
            "1px solid " +
            (!formReady ? "var(--border-danger)" : "oklch(0.55 0.16 155)"),
          borderRadius: 4,
          color: !formReady ? "var(--text-muted)" : "oklch(0.92 0.1 155)",
          fontFamily: "inherit",
          fontSize: 11,
          fontWeight: 700,
          cursor: !formReady ? "default" : "pointer",
          transition: "all 0.2s",
          display: "flex",
          alignItems: "center",
          gap: 7,
          pointerEvents: !formReady ? "none" : undefined,
        }}
      >
        {!formReady && (
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: "var(--text-danger)",
              flexShrink: 0,
            }}
          />
        )}
        Launch Scheduler
      </button>
    );
    launchControl = !formReady ? (
      <HelpTip text={blocking.map((c) => "- " + c.label).join("\n")} width={520}>{launchBtn}</HelpTip>
    ) : (
      launchBtn
    );
  } else if (phase === "ready") {
    launchControl = (
      <button
        onClick={handleDestroy}
        disabled={!hasCredentials}
        style={_destroyBtnStyle(!hasCredentials)}
      >
        Destroy Cluster{!hasCredentials ? " (missing credentials)" : ""}
      </button>
    );
  } else if (isRunning) {
    launchControl = (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div
          style={{
            padding: "6px 12px",
            background:
              phase === "destroying"
                ? "rgba(255,80,60,0.04)"
                : "rgba(0,200,224,0.04)",
            border:
              "1px solid " +
              (phase === "destroying"
                ? "var(--border-danger)"
                : "var(--border-accent)"),
            borderRadius: 4,
            color:
              phase === "destroying"
                ? "var(--text-danger)"
                : "var(--text-muted)",
            fontSize: 11,
          }}
        >
          {phase === "destroying" ? "Tearing down…" : "Deploying…"}
        </div>
        <button
          onClick={handleAbort}
          style={{
            padding: "6px 12px",
            background: "transparent",
            border: "1px solid rgba(255,160,60,0.3)",
            borderRadius: 4,
            color: "var(--text-warning)",
            fontFamily: "inherit",
            fontSize: 11,
            cursor: "pointer",
            transition: "border-color 0.15s, color 0.15s",
            flexShrink: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = "rgba(255,160,60,0.6)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = "rgba(255,160,60,0.3)";
          }}
        >
          ✕ Abort
        </button>
      </div>
    );
  }

  return (
    <div
      style={{
        height: "100%",
        background: "var(--bg-page)",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <TopNav
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        theme={theme}
        setTheme={setTheme}
        showPostLaunch={
          (phase !== "idle" && phase !== "error") ||
          activeTab === "deployment"
        }
        schedulerReady={phase === "ready" && !!provState?.scheduler_address}
        launchControl={launchControl}
      />

      {/* ── Config Tab ── */}
      <div
        style={{
          display: activeTab === "config" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div
          style={{
            flex: 1,
            padding: "20px 28px",
            overflowY: "auto",
            display: "flex",
            flexDirection: "column",
            gap: 16,
          }}
        >
          {/* Three columns */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "320px 340px 1fr",
              gap: 16,
              alignItems: "start",
            }}
          >
            {/* Column 1: Credentials + General */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox title="Credentials">
                <div
                  style={{
                    display: "flex",
                    borderBottom: "1px solid var(--border-accent)",
                    gap: 0,
                    marginBottom: 2,
                  }}
                >
                  {[
                    ["aws", "AWS"],
                    ["ibm", "IBM"],
                    ["oci", "OCI"],
                  ].map(([id, lbl]) => {
                    const active = id === credTab;
                    const disabled = id === "ibm";
                    return (
                      <button
                        key={id}
                        disabled={disabled}
                        onClick={() => !disabled && setCredTab(id)}
                        style={{
                          padding: "5px 12px",
                          fontFamily: "inherit",
                          fontSize: 10,
                          letterSpacing: "0.08em",
                          textTransform: "uppercase",
                          cursor: disabled ? "default" : "pointer",
                          border: "none",
                          marginBottom: -1,
                          borderBottom: active
                            ? "2px solid var(--tab-active)"
                            : "2px solid transparent",
                          background: "transparent",
                          color: active
                            ? "var(--text-label)"
                            : "var(--text-dim)",
                          opacity: disabled ? 0.35 : 1,
                        }}
                      >
                        {lbl}
                      </button>
                    );
                  })}
                </div>
                {IS_DEV && (
                  <div
                    style={{
                      fontSize: 10,
                      color: "var(--text-warning)",
                      background: "rgba(255,202,22,0.06)",
                      border: "1px solid rgba(255,202,22,0.25)",
                      borderRadius: 3,
                      padding: "6px 10px",
                      lineHeight: 1.5,
                    }}
                  >
                    <strong>DEV mode:</strong> credentials are saved to sessionStorage on every
                    keystroke. Browsers may flush sessionStorage to disk for crash-recovery, so
                    plaintext secrets can persist beyond a tab close.
                  </div>
                )}
                {credTab === "aws" && (
                  <>
                    <div>
                      <Label help="The AWS region where your cluster will be deployed.">
                        AWS Region
                      </Label>
                      <RegionSelect value={region} onChange={setRegion} />
                    </div>
                    <div
                      style={{ display: "flex", flexDirection: "column", gap: 6 }}
                    >
                      <div
                        style={{
                          background: "var(--bg-surface)",
                          border: "1px solid var(--border-accent)",
                          borderRadius: 3,
                          padding: "8px 10px",
                          display: "flex",
                          alignItems: "center",
                        }}
                      >
                        <span
                          style={{
                            fontSize: 10,
                            color: "var(--text-muted)",
                            marginRight: 8,
                            flexShrink: 0,
                          }}
                        >
                          Key ID
                        </span>
                        <SecretInput
                          value={accessKeyId}
                          onChange={setAKI}
                          placeholder="AKIA…"
                          style={{
                            flex: 1,
                            fontSize: 12,
                            color: "var(--text-primary)",
                          }}
                        />
                      </div>
                      <div
                        style={{
                          background: "var(--bg-surface)",
                          border: "1px solid var(--border-accent)",
                          borderRadius: 3,
                          padding: "8px 10px",
                          display: "flex",
                          alignItems: "center",
                        }}
                      >
                        <span
                          style={{
                            fontSize: 10,
                            color: "var(--text-muted)",
                            marginRight: 8,
                            flexShrink: 0,
                          }}
                        >
                          Secret
                        </span>
                        <SecretInput
                          value={secretKey}
                          onChange={setSK}
                          placeholder="wJalr…"
                          style={{
                            flex: 1,
                            fontSize: 12,
                            color: "var(--text-primary)",
                          }}
                        />
                      </div>
                      <a
                        href="https://console.aws.amazon.com/iam/home#/security_credentials"
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          fontSize: 10,
                          color: "var(--text-muted)",
                          textDecoration: "none",
                          alignSelf: "flex-end",
                        }}
                        onMouseOver={(e) =>
                          (e.currentTarget.style.color = "var(--text-accent)")
                        }
                        onMouseOut={(e) =>
                          (e.currentTarget.style.color = "var(--text-muted)")
                        }
                      >
                        Generate access keys in AWS Console ↗
                      </a>
                      <span
                        style={{
                          fontSize: 10,
                          color: "var(--text-dim)",
                          lineHeight: 1.5,
                        }}
                      >
                        Your credentials are used from this browser to provision AWS
                        resources and are made available to the scheduler instance
                        for worker management. They are not stored by this
                        application.
                      </span>
                    </div>
                  </>
                )}
                {credTab === "oci" && (
                  <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                    <div>
                      <Label help={"Your OCI user OCID. In the OCI Console, click your profile avatar (top-right) > User Settings. The OCID is listed under User Information."}>User OCID</Label>
                      <div style={{ ...inp, display: "flex", alignItems: "center" }}>
                        <SecretInput
                          value={ociUserId}
                          onChange={setOciUserId}
                          placeholder="ocid1.user.oc1..aaa..."
                          style={{ flex: 1, fontSize: 12, color: "var(--text-primary)" }}
                        />
                      </div>
                    </div>
                    <div>
                      <Label help={"Your tenancy OCID. In the OCI Console, open the navigation menu > Governance & Administration > Tenancy Details. The OCID is listed at the top."}>Tenancy OCID</Label>
                      <div style={{ ...inp, display: "flex", alignItems: "center" }}>
                        <SecretInput
                          value={ociTenancyId}
                          onChange={setOciTenancyId}
                          placeholder="ocid1.tenancy.oc1..aaa..."
                          style={{ flex: 1, fontSize: 12, color: "var(--text-primary)" }}
                        />
                      </div>
                    </div>
                    <div>
                      <Label help={"The fingerprint of your API signing key. In the OCI Console, go to User Settings > Tokens and Keys > API Keys. The fingerprint (format: aa:bb:cc:...) is shown next to your key. If you don't have one, click Add API Key to generate a key pair."}>Fingerprint</Label>
                      <div style={{ ...inp, display: "flex", alignItems: "center" }}>
                        <SecretInput
                          value={ociFingerprint}
                          onChange={setOciFingerprint}
                          placeholder="aa:bb:cc:dd:ee:ff"
                          style={{ flex: 1, fontSize: 12, color: "var(--text-primary)" }}
                        />
                      </div>
                    </div>
                    <div>
                      <Label help={"The private key that pairs with your API key fingerprint. In the OCI Console, go to User Settings > Tokens and Keys > API Keys > Add API Key, then download the private key file. Paste the full contents of the .pem file here, including the BEGIN/END lines."}>Private Key (PEM)</Label>
                      <textarea
                        value={ociPrivateKey}
                        onChange={(e) => setOciPrivateKey(e.target.value)}
                        placeholder={"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"}
                        style={{
                          ...inp,
                          fontFamily: "monospace",
                          fontSize: 10,
                          resize: "vertical",
                          minHeight: 80,
                          lineHeight: 1.5,
                          boxSizing: "border-box",
                        }}
                      />
                    </div>
                    <span
                      style={{ fontSize: 10, color: "var(--text-dim)", lineHeight: 1.5 }}
                    >
                      OCI credentials are written to ~/.oci/config on the scheduler
                      instance so the worker manager can authenticate with OCI.
                      They are not stored by this application.
                    </span>
                  </div>
                )}
              </PanelBox>

              <PanelBox title="General Options">
                {IS_ADVANCED && (
                  <div>
                    <Label
                      help={
                        "WebSocket - connect to your cluster from a browser or any WebSocket client.\n---\nTCP - direct socket connection; slightly lower overhead."
                      }
                    >
                      Transport Protocol
                    </Label>
                    <TogglePair
                      options={[
                        ["ws", "WS"],
                        ["tcp", "TCP"],
                      ]}
                      value={transport}
                      onSelect={setTransport}
                    />
                  </div>
                )}
                <div>
                  <Label help="Python version installed via uv on the scheduler and all workers.">
                    Python Version
                  </Label>
                  <input
                    value={pythonVersion}
                    onChange={(e) => setPyVer(e.target.value)}
                    style={inp}
                    placeholder="3.14"
                  />
                </div>
              </PanelBox>
            </div>

            {/* Column 2: Scheduler EC2 + Policy */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox title="Scheduler (AWS-only)">
                <div>
                  <Label help="EC2 instance type for the scheduler. Compute-optimized (c5/c6i) works well for most deployments.">
                    Instance Type
                  </Label>
                  <InstancePicker
                    value={schedulerType}
                    onChange={setSchedulerType}
                    defaultCat="all"
                  />
                </div>
                <div
                  style={{
                    padding: "10px 12px",
                    background: "rgba(0,255,136,0.04)",
                    border: "1px solid var(--border-success)",
                    borderRadius: 3,
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "baseline",
                  }}
                >
                  <span
                    style={{
                      fontSize: 10,
                      color: "var(--text-muted)",
                    }}
                  >
                    Cost
                  </span>
                  <span
                    style={{
                      fontSize: 13,
                      fontWeight: 600,
                      color: "var(--text-success)",
                    }}
                  >
                    USD {schedulerInst.price.toFixed(2)}/h
                  </span>
                </div>
                {IS_ADVANCED && (
                  <div
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      gap: 10,
                    }}
                  >
                    <div>
                      <Label>Scheduler Port</Label>
                      <NumericStepper
                        value={schedulerPort}
                        onChange={setSchedPort}
                        min={1024}
                        max={65535}
                        width={80}
                      />
                    </div>
                    <div>
                      <Label>Object Storage Port</Label>
                      <NumericStepper
                        value={objectStoragePort}
                        onChange={setObjPort}
                        min={1024}
                        max={65535}
                        width={80}
                      />
                    </div>
                    {portConflicts.length > 0 && (
                      <div
                        style={{
                          color: "var(--text-danger)",
                          fontSize: 11,
                          lineHeight: 1.5,
                        }}
                      >
                        {portConflicts.map((msg, i) => (
                          <div key={i}>{msg}</div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
                <div>
                  <Label
                    help={
                      "- Installed on the scheduler instance\n- Shared by native worker manager workers (same instance)\n- opengris-scaler must be included"
                    }
                  >
                    requirements.txt
                  </Label>
                  <textarea
                    value={schedulerRequirements}
                    onChange={(e) => setSchedulerReqs(e.target.value)}
                    spellCheck={false}
                    style={{
                      width: "100%",
                      background: "var(--bg-surface)",
                      border: "1px solid var(--border-accent)",
                      borderRadius: 3,
                      padding: "7px 10px",
                      color: "var(--text-primary)",
                      fontFamily: "inherit",
                      fontSize: 11,
                      outline: "none",
                      resize: "vertical",
                      minHeight: 72,
                      lineHeight: 1.6,
                    }}
                  />
                </div>
              </PanelBox>

              <PanelBox title="Policy">
                <div>
                  <Label help="Policy engine that controls task allocation and worker scaling.">
                    Engine
                  </Label>
                  <PolicyDropdown value={policy} onChange={setPolicy} />
                  {policy === "waterfall_v1" && (
                    <div
                      style={{
                        marginTop: 8,
                        fontSize: 10,
                        color: "var(--text-dim)",
                        lineHeight: 1.5,
                      }}
                    >
                      Priority is based on ordering in the Worker Managers pane. Drag to reorder.
                    </div>
                  )}
                </div>
              </PanelBox>
            </div>

            {/* Column 3: Worker Managers + Cost Summary */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <PanelBox
                title={`Worker Managers (${workerManagers.length})`}
                style={{ gap: 8, padding: "16px 22px" }}
              >
                <div
                  style={{
                    display: "flex",
                    marginLeft: -22,
                    marginRight: -22,
                    borderTop: "1px solid var(--border-accent)",
                  }}
                >
                  {/* vertical tab list */}
                  <div
                    style={{
                      width: 130,
                      borderRight: "1px solid var(--border-accent)",
                      display: "flex",
                      flexDirection: "column",
                      flexShrink: 0,
                      overflowY: "auto",
                      maxHeight: 420,
                      alignSelf: "flex-start",
                    }}
                  >
                    {workerManagers.map((wm, wmIdx) => (
                      <div
                        key={wm.id}
                        onDragOver={(e) => {
                          e.preventDefault();
                          e.dataTransfer.dropEffect = "move";
                          if (wm.id !== draggedWmId) setDragOverWmId(wm.id);
                        }}
                        onDragLeave={(e) => {
                          if (!e.currentTarget.contains(e.relatedTarget))
                            setDragOverWmId(null);
                        }}
                        onDrop={(e) => {
                          e.preventDefault();
                          if (draggedWmId && wm.id !== draggedWmId) {
                            setWorkerManagers((prev) => {
                              const from = prev.findIndex((w) => w.id === draggedWmId);
                              const to = prev.findIndex((w) => w.id === wm.id);
                              if (from === -1 || to === -1) return prev;
                              const next = [...prev];
                              const [item] = next.splice(from, 1);
                              next.splice(to, 0, item);
                              return next;
                            });
                          }
                          setDraggedWmId(null);
                          setDragOverWmId(null);
                        }}
                        onDragEnd={() => {
                          setDraggedWmId(null);
                          setDragOverWmId(null);
                        }}
                        style={{
                          display: "flex",
                          alignItems: "stretch",
                          background:
                            dragOverWmId === wm.id
                              ? "rgba(0,200,224,0.18)"
                              : selectedWmId === wm.id
                                ? "rgba(0,200,224,0.1)"
                                : "transparent",
                          borderLeft:
                            selectedWmId === wm.id
                              ? "2px solid var(--tab-active)"
                              : "2px solid transparent",
                          borderBottom: "1px solid rgba(255,255,255,0.04)",
                          transition: "background 0.1s",
                          opacity: draggedWmId === wm.id ? 0.4 : 1,
                        }}
                      >
                        {workerManagers.length > 1 && (
                          <div
                            draggable={true}
                            onDragStart={(e) => {
                              setDraggedWmId(wm.id);
                              e.dataTransfer.effectAllowed = "move";
                              e.dataTransfer.setData("text/plain", wm.id);
                            }}
                            style={{
                              cursor: "grab",
                              display: "flex",
                              alignItems: "center",
                              padding: "0 3px 0 7px",
                              flexShrink: 0,
                              color: "var(--text-dim)",
                              userSelect: "none",
                            }}
                            onMouseEnter={(e) => {
                              e.currentTarget.style.color = "var(--text-muted)";
                            }}
                            onMouseLeave={(e) => {
                              e.currentTarget.style.color = "var(--text-dim)";
                            }}
                          >
                            <svg
                              width="8"
                              height="12"
                              viewBox="0 0 8 12"
                              fill="currentColor"
                              style={{ display: "block" }}
                            >
                              <circle cx="2" cy="2" r="1.5" />
                              <circle cx="6" cy="2" r="1.5" />
                              <circle cx="2" cy="6" r="1.5" />
                              <circle cx="6" cy="6" r="1.5" />
                              <circle cx="2" cy="10" r="1.5" />
                              <circle cx="6" cy="10" r="1.5" />
                            </svg>
                          </div>
                        )}
                        <button
                          title={wm.id}
                          onClick={() => setSelectedWmId(wm.id)}
                          style={{
                            flex: 1,
                            display: "flex",
                            alignItems: "center",
                            gap: 4,
                            background: "transparent",
                            border: "none",
                            color:
                              selectedWmId === wm.id
                                ? "var(--text-accent)"
                                : "var(--text-muted)",
                            fontFamily: "inherit",
                            fontSize: 10,
                            padding: workerManagers.length > 1 ? "10px 4px 10px 4px" : "10px 4px 10px 10px",
                            textAlign: "left",
                            cursor: "pointer",
                            letterSpacing: "0.05em",
                            transition: "color 0.12s",
                            minWidth: 0,
                            overflow: "hidden",
                          }}
                        >
                          {policy === "waterfall_v1" && (
                            <span
                              style={{
                                fontSize: 8,
                                fontWeight: 700,
                                lineHeight: 1,
                                color: "var(--accent-cyan)",
                                background: "rgba(0,200,224,0.12)",
                                borderRadius: 2,
                                padding: "2px 3px",
                                flexShrink: 0,
                              }}
                            >
                              {wmIdx + 1}
                            </span>
                          )}
                          <span
                            style={{
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                              whiteSpace: "nowrap",
                            }}
                          >
                            {wm.id}
                          </span>
                        </button>
                        {workerManagers.length > 1 && (
                          <button
                            onClick={() => removeWorkerManager(wm.id)}
                            title="Remove"
                            style={{
                              background: "transparent",
                              border: "none",
                              color: "var(--text-muted)",
                              fontFamily: "inherit",
                              fontSize: 9,
                              padding: 0,
                              margin: "0 7px",
                              cursor: "pointer",
                              flexShrink: 0,
                              alignSelf: "center",
                              display: "flex",
                              alignItems: "center",
                              transition: "color 0.12s",
                            }}
                            onMouseEnter={(e) => {
                              e.currentTarget.style.color = "var(--text-danger)";
                              e.currentTarget.querySelector("span").style.borderColor = "var(--border-danger)";
                              e.currentTarget.querySelector("span").style.background = "rgba(229,72,77,0.08)";
                            }}
                            onMouseLeave={(e) => {
                              e.currentTarget.style.color = "var(--text-muted)";
                              e.currentTarget.querySelector("span").style.borderColor = "var(--border-accent)";
                              e.currentTarget.querySelector("span").style.background = "transparent";
                            }}
                          >
                            <span
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                justifyContent: "center",
                                width: 14,
                                height: 14,
                                border: "1px solid var(--border-accent)",
                                borderRadius: 2,
                                transition: "border-color 0.12s, background 0.12s",
                              }}
                            >
                              ✕
                            </span>
                          </button>
                        )}
                      </div>
                    ))}
                    <button
                      onClick={addWorkerManager}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.background =
                          "rgba(0,200,224,0.08)";
                        e.currentTarget.style.color = "var(--text-accent)";
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.background = "transparent";
                        e.currentTarget.style.color = "var(--accent-cyan)";
                      }}
                      style={{
                        background: "transparent",
                        border: "none",
                        borderTop: "1px dashed rgba(0,200,224,0.2)",
                        color: "var(--accent-cyan)",
                        fontFamily: "inherit",
                        fontSize: 10,
                        padding: "10px 10px",
                        cursor: "pointer",
                        textAlign: "left",
                        marginTop: "auto",
                        letterSpacing: "0.05em",
                        transition: "background 0.12s, color 0.12s",
                      }}
                    >
                      + Add
                    </button>
                  </div>
                  {/* selected card */}
                  <div style={{ flex: 1, padding: "14px 16px" }}>
                    {workerManagers
                      .filter((wm) => wm.id === selectedWmId)
                      .map((wm) => (
                        <React.Fragment key={wm._uid}>
                          <WorkerManagerCard
                            wm={wm}
                            onChange={(updated) => {
                              if (updated.id !== wm.id)
                                setSelectedWmId(updated.id);
                              updateWorkerManager(wm.id, updated);
                            }}
                            onRemove={() => removeWorkerManager(wm.id)}
                            allInstances={allInstances}
                            canRemove={workerManagers.length > 1}
                            fullWidth={true}
                          />
                        </React.Fragment>
                      ))}
                  </div>
                </div>
              </PanelBox>

              <PanelBox title="Cost Summary">
                {workerManagers.map((wm, idx) => {
                  const label = wm.id || `(wm ${idx + 1})`;
                  if (wm.type === "orb_aws_ec2") {
                    const inst = allInstances.find((i) => i.type === wm.instanceType) || { price: 0 };
                    const count =
                      wm.capMode === "instances"
                        ? Math.max(0, wm.instanceCap || 0)
                        : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
                    return (
                      <div key={wm._uid} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
                          {label} · {count}× {wm.instanceType}
                        </span>
                        <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                          USD {(count * inst.price).toFixed(2)}/h
                        </span>
                      </div>
                    );
                  }
                  if (wm.type === "oci_raw") {
                    const shape = wm.ociShape || "CI.Standard.A1.Flex";
                    const shapeName = shape === "CI.Standard.A1.Flex" ? "ARM - Ampere A1" : "x86 - Standard E4";
                    const pricing = OCI_SHAPE_PRICING[shape] || OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
                    const costPerInstance = pricing.ocpuPrice * (wm.ociOcpus || 4) + pricing.memPrice * (wm.ociMemoryGb || 8);
                    const count = wm.capMode === "instances"
                      ? Math.max(0, wm.instanceCap || 0)
                      : Math.max(0, Math.floor((wm.budgetCap || 0) / (costPerInstance || 1)));
                    return (
                      <div key={wm._uid} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
                          {label} · {count}× {shapeName} · {wm.ociOcpus || 4} OCPU · {wm.ociMemoryGb || 8}GB
                        </span>
                        <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                          USD {(count * costPerInstance).toFixed(2)}/h
                        </span>
                      </div>
                    );
                  }
                  return (
                    <div key={wm._uid} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                      <span style={{ fontSize: 10, color: "var(--text-muted)" }}>{label}</span>
                      <span style={{ fontSize: 11, color: "var(--text-dim)", fontStyle: "italic" }}>n/a</span>
                    </div>
                  );
                })}
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "baseline",
                  }}
                >
                  <span
                    style={{
                      fontSize: 10,
                      color: "var(--text-muted)",
                    }}
                  >
                    Scheduler · {schedulerType}
                  </span>
                  <span
                    style={{ fontSize: 12, color: "var(--text-secondary)" }}
                  >
                    USD {schedulerInst.price.toFixed(2)}/h
                  </span>
                </div>
                <div
                  style={{
                    borderTop: "1px solid var(--border-success)",
                    paddingTop: 10,
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "baseline",
                  }}
                >
                  <span
                    style={{
                      fontSize: 11,
                      color: "var(--text-accent)",
                      fontWeight: 600,
                    }}
                  >
                    Max est. total
                  </span>
                  <span
                    style={{
                      fontSize: 16,
                      fontWeight: 700,
                      color: "var(--text-success)",
                    }}
                  >
                    USD {totalCostPerHr.toFixed(2)}/h
                  </span>
                </div>
              </PanelBox>
            </div>
          </div>

          <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
              <button
                onClick={handleDownloadConfig}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  color: "var(--text-accent)",
                  fontFamily: "inherit",
                  fontSize: 10,
                  padding: 0,
                  letterSpacing: "0.06em",
                  textDecoration: "underline",
                  textDecorationColor: "var(--border-accent)",
                }}
              >
                Download config.toml
              </button>
              <button
                onClick={() => loadConfigInputRef.current && loadConfigInputRef.current.click()}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  color: "var(--text-muted)",
                  fontFamily: "inherit",
                  fontSize: 10,
                  padding: 0,
                  letterSpacing: "0.06em",
                  textDecoration: "underline",
                  textDecorationColor: "var(--border-accent)",
                }}
              >
                Load config.toml
              </button>
              <input
                ref={loadConfigInputRef}
                type="file"
                accept=".toml"
                style={{ display: "none" }}
                onChange={handleLoadConfig}
              />
            </div>
        </div>
      </div>

      {/* ── Deployment Tab ── */}
      <div
        style={{
          display: activeTab === "deployment" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div
          style={{
            flex: 1,
            padding: "20px 28px",
            display: "grid",
            gridTemplateColumns: "1fr 600px",
            gap: 20,
            minHeight: 0,
            overflow: "hidden",
          }}
        >
          {/* Left: terminal only */}
          <LiveTerminal
            lines={log}
            isRunning={isRunning}
            bare
            style={{ minHeight: 0 }}
          />
          {/* Right: active deployment card */}
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 12,
              overflowY: "auto",
              minHeight: 0,
            }}
          >
            {provState && phase !== "destroying" && (
              <div
                style={{
                  animation: "fadeSlideIn 0.3s ease",
                  display: "flex",
                  flexDirection: "column",
                  gap: 12,
                }}
              >
                {phase === "ready" && log.length === 0 && (
                  <div
                    style={{
                      padding: "10px 14px",
                      background: "var(--bg-surface)",
                      border: "1px solid var(--border-accent)",
                      borderRadius: 3,
                      fontSize: 11,
                      color: "var(--text-muted)",
                    }}
                  >
                    Deployment loaded from saved state.
                  </div>
                )}
                <DeploymentCard
                  state={provState}
                  onDownload={() =>
                    downloadText(
                      "scaler-state-" + provState.name_suffix + ".json",
                      JSON.stringify(provState, null, 2),
                    )
                  }
                  isRunning={isRunning}
                  keyMaterial={keyMaterial}
                />
                <GettingStartedCard
                  schedulerAddress={provState.scheduler_address}
                  ready={phase === "ready"}
                />
              </div>
            )}
            {phase === "error" && (
              <button
                onClick={handleReset}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  color: "var(--text-muted)",
                  fontFamily: "inherit",
                  fontSize: 10,
                  padding: 0,
                  letterSpacing: "0.06em",
                }}
              >
                ← Clear state
              </button>
            )}
          </div>
        </div>
      </div>

      {/* ── Scheduler Logs Tab ── */}
      <div
        style={{
          display: activeTab === "logs" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div
          style={{
            flex: 1,
            minHeight: 0,
            padding: "20px 28px",
            display: "flex",
            flexDirection: "column",
          }}
        >
          {!provState?.instance_id ? (
            <div style={{ color: "var(--text-muted)", fontSize: 12 }}>
              No instance deployed yet.
            </div>
          ) : (
            <SchedulerLogTerminal
              instanceId={provState.instance_id}
              region={provState.region}
              credentials={{ accessKeyId, secretKey }}
              isActive={activeTab === "logs"}
            />
          )}
        </div>
      </div>

      {/* ── Worker Monitor Tab ── */}
      <div
        style={{
          display: activeTab === "worker-monitor" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        {!provState?.worker_monitor_address ? (
          <div
            style={{
              padding: "20px 28px",
              color: "var(--text-muted)",
              fontSize: 12,
            }}
          >
            Worker Monitor address not yet available.
          </div>
        ) : (
          <>
            <div
              style={{
                padding: "8px 14px",
                background: "var(--bg-panel)",
                borderBottom: "1px solid var(--border-accent)",
                display: "flex",
                gap: 10,
                alignItems: "center",
                flexShrink: 0,
              }}
            >
              <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                {provState.worker_monitor_address}
              </span>
              <a
                href={provState.worker_monitor_address}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  fontSize: 10,
                  color: "var(--text-accent)",
                  border: "1px solid var(--border-accent)",
                  borderRadius: 3,
                  padding: "2px 8px",
                  textDecoration: "none",
                }}
              >
                Open in new tab
              </a>
              {workerMonitorReady ? (
                <span style={{ fontSize: 10, color: "var(--text-success)" }}>
                  server ready
                </span>
              ) : (
                <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
                  waiting for server… {workerMonitorElapsed}s
                </span>
              )}
            </div>
            {workerMonitorReady ? (
              <iframe
                src={provState.worker_monitor_address}
                style={{
                  flex: 1,
                  border: "none",
                  background: "var(--bg-page)",
                }}
                title="Scaler Worker Monitor"
              />
            ) : (
              <div
                style={{
                  flex: 1,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  flexDirection: "column",
                  gap: 10,
                  color: "var(--text-muted)",
                }}
              >
                <div style={{ fontSize: 13 }}>
                  Waiting for Worker Monitor server to start
                </div>
                <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
                  {workerMonitorElapsed}s elapsed · retrying every 5s
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {/* ── Try it Tab ── */}
      <div
        style={{
          display: activeTab === "try-it" ? "flex" : "none",
          flex: 1,
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <TryItTab
          isActive={activeTab === "try-it"}
          theme={theme}
          schedulerAddress={phase === "ready" ? provState?.scheduler_address : ""}
        />
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
