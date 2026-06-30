"use strict";

const _EC2_TRUST_POLICY = JSON.stringify({
  Version: "2012-10-17",
  Statement: [
    {
      Effect: "Allow",
      Principal: { Service: "ec2.amazonaws.com" },
      Action: "sts:AssumeRole",
    },
  ],
});

const _ORB_INLINE_POLICY = JSON.stringify({
  Version: "2012-10-17",
  Statement: [
    {
      Effect: "Allow",
      Action: [
        "ec2:*",
        "autoscaling:*",
        "iam:GetRole",
        "iam:PassRole",
        "ssm:GetParameter",
        "sts:GetCallerIdentity",
      ],
      Resource: "*",
    },
  ],
});

const _OCI_SHAPE_PRICING = {
  "CI.Standard.A1.Flex": { ocpuPrice: 0.013106, memPrice: 0.0019659 },
  "CI.Standard.E4.Flex": { ocpuPrice: 0.032765, memPrice: 0.0019659 },
};

function randomSuffix(n) {
  n = n || 8;
  var chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from({ length: n }, function () {
    return chars[Math.floor(Math.random() * chars.length)];
  }).join("");
}

function sleep(ms, signal) {
  return new Promise(function (resolve, reject) {
    if (signal && signal.aborted)
      return reject(new DOMException("Aborted", "AbortError"));
    var timer = setTimeout(resolve, ms);
    if (signal)
      signal.addEventListener(
        "abort",
        function () {
          clearTimeout(timer);
          reject(new DOMException("Aborted", "AbortError"));
        },
        { once: true },
      );
  });
}

// Race an AWS SDK promise against an AbortSignal so waiters can be interrupted.
function withAbort(promise, signal) {
  if (!signal) return promise;
  return Promise.race([
    promise,
    new Promise(function (_, reject) {
      if (signal.aborted)
        return reject(new DOMException("Aborted", "AbortError"));
      signal.addEventListener(
        "abort",
        function () {
          reject(new DOMException("Aborted", "AbortError"));
        },
        { once: true },
      );
    }),
  ]);
}

function _isPermanentError(err) {
  var codes = [
    "InvalidPermission.Duplicate",
    "EntityAlreadyExists",
    "InvalidKeyPair.Duplicate",
    "InvalidGroup.Duplicate",
  ];
  return codes.indexOf(err.code) >= 0;
}

// Retry an AWS operation up to maxAttempts times with exponential back-off.
// Throws a RetryPausedError (name = "RetryPausedError") after all attempts fail,
// so callers can distinguish "paused waiting for user" from ordinary errors.
async function retrying(addLog, signal, fn, maxAttempts) {
  maxAttempts = maxAttempts || 3;
  for (var attempt = 1; attempt <= maxAttempts; attempt++) {
    if (signal && signal.aborted)
      throw new DOMException("Aborted", "AbortError");
    try {
      return await withAbort(fn(), signal);
    } catch (err) {
      if (err.name === "AbortError") throw err;
      if (_isPermanentError(err)) throw err;
      if (attempt === maxAttempts) {
        var paused = new Error(err.message);
        paused.name = "RetryPausedError";
        throw paused;
      }
      var delayMs = 2000 * Math.pow(2, attempt - 1);
      addLog("  ! " + err.message, "warn");
      addLog(
        "  → Retrying in " +
          Math.round(delayMs / 1000) +
          "s… (attempt " +
          (attempt + 1) +
          " of " +
          maxAttempts +
          ")",
        "dim",
      );
      await sleep(delayMs, signal);
    }
  }
}

function downloadText(filename, content) {
  var blob = new Blob([content], { type: "text/plain" });
  var url = URL.createObjectURL(blob);
  var a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// Builds one [[worker_manager]] TOML table. `ctx` carries the proto/port/address values that
// differ between the download template (placeholder strings like <PRIVATE_IP>) and the actual
// EC2 user-data script (bash variables like $PRIVATE_IP, resolved at instance boot).
function buildWorkerManagerTable(wm, cfg, ctx) {
  var table = {
    type: wm.type,
    scheduler_address: `${ctx.proto}://127.0.0.1:${ctx.sp}${ctx.wsSlash}`,
    worker_manager_id: wm.id,
  };

  if (wm.type === "orb_aws_ec2") {
    var req = (wm.requirements || "").trim();
    var inst = (window.SCALER_INSTANCES || []).find(function (i) { return i.type === wm.instanceType; });
    if (!inst || inst.vcpu == null) throw new Error("Unknown EC2 instance type: " + wm.instanceType);
    var orbDerivedCount = wm.capMode === "instances"
      ? Math.max(0, wm.instanceCap || 0)
      : Math.max(0, Math.floor((wm.budgetCap || 0) / (inst.price || 1)));
    Object.assign(table, {
      worker_scheduler_address: `${ctx.proto}://${ctx.privateIp}:${ctx.sp}${ctx.wsSlash}`,
      object_storage_address: `${ctx.proto}://${ctx.privateIp}:${ctx.op}${ctx.wsSlash}`,
      python_version: cfg.pythonVersion,
      requirements_txt: TOML.multiline.basic(req + "\n"),
      instance_type: wm.instanceType,
      max_task_concurrency: orbDerivedCount * inst.vcpu,
      aws_region: cfg.region,
      key_name: `scaler-key-${ctx.nameSuffix}`,
      subnet_id: ctx.subnetId,
      security_group_ids: TOML.inline([ctx.securityGroupId]),
      logging_level: "INFO",
      instance_tags: TOML.inline({ "scaler-deployment": ctx.nameSuffix }),
    });
    table.network_backend = cfg.networkBackend || "ymq";
  } else if (wm.type === "aws_raw_ecs") {
    Object.assign(table, {
      aws_region: cfg.region,
      ecs_cluster: wm.ecsCluster || "scaler-cluster",
      ecs_task_image: wm.ecsTaskImage || "",
      ecs_subnets: wm.ecsSubnets || "",
      ecs_task_definition: wm.ecsTaskDefinition || "scaler-task-definition",
      ecs_task_cpu: wm.ecsTaskCpu || 4,
      ecs_task_memory: wm.ecsTaskMemory || 30,
      ecs_python_version: cfg.pythonVersion,
    });
    if (wm.requirements) table.ecs_python_requirements = wm.requirements;
  } else if (wm.type === "aws_hpc") {
    Object.assign(table, {
      aws_region: cfg.region,
      job_queue: wm.jobQueue || "",
      job_definition: wm.jobDefinition || "",
      s3_bucket: wm.s3Bucket || "",
      s3_prefix: wm.s3Prefix || "scaler-tasks",
      max_concurrent_jobs: wm.maxConcurrentJobs || 100,
      job_timeout_minutes: wm.jobTimeoutMinutes || 60,
    });
  } else if (wm.type === "oci_raw") {
    var ociRawReq = (wm.requirements || "").trim();
    var ociRawPricing = _OCI_SHAPE_PRICING[wm.ociShape || "CI.Standard.A1.Flex"] || _OCI_SHAPE_PRICING["CI.Standard.A1.Flex"];
    var ociRawCostPerInstance = ociRawPricing.ocpuPrice * wm.ociOcpus + ociRawPricing.memPrice * wm.ociMemoryGb;
    var ociRawDerivedCount = wm.capMode === "instances"
      ? Math.max(0, wm.instanceCap || 0)
      : Math.max(0, Math.floor((wm.budgetCap || 0) / (ociRawCostPerInstance || 1)));
    Object.assign(table, {
      worker_scheduler_address: `${ctx.proto}://$PUBLIC_IP:${ctx.sp}${ctx.wsSlash}`,
      oci_region: wm.ociRegion || "us-ashburn-1",
      compartment_id: wm.ociCompartmentId || "",
      availability_domain: wm.ociAvailabilityDomain || "",
      subnet_id: wm.ociSubnetId || "",
      container_image: wm.ociContainerImage || "",
      instance_shape: wm.ociShape || "CI.Standard.E4.Flex",
      instance_ocpus: wm.ociOcpus,
      instance_memory_gb: wm.ociMemoryGb,
      max_task_concurrency: ociRawDerivedCount * wm.ociOcpus,
      python_version: cfg.pythonVersion,
      requirements_txt: TOML.multiline.basic(ociRawReq + "\n"),
    });
  } else if (wm.type === "oci_hpc") {
    Object.assign(table, {
      worker_scheduler_address: `${ctx.proto}://$PUBLIC_IP:${ctx.sp}${ctx.wsSlash}`,
      oci_region: wm.ociRegion || "us-ashburn-1",
      compartment_id: wm.ociCompartmentId || "",
      availability_domain: wm.ociAvailabilityDomain || "",
      subnet_id: wm.ociSubnetId || "",
      container_image: wm.ociContainerImage || "",
      object_storage_namespace: wm.ociObjectStorageNamespace || "",
      object_storage_bucket: wm.ociObjectStorageBucket || "",
      object_storage_prefix: wm.ociObjectStoragePrefix || "scaler-tasks",
      instance_ocpus: wm.ociOcpus || 1,
      instance_memory_gb: wm.ociMemoryGb || 6,
      base_concurrency: wm.ociMaxConcurrentJobs || 100,
      job_timeout_seconds: (wm.ociJobTimeoutMinutes || 60) * 60,
    });
  } else if (wm.type === "baremetal_native") {
    Object.assign(table, {
      mode: wm.mode || "fixed",
      object_storage_address: `${ctx.proto}://127.0.0.1:${ctx.op}${ctx.wsSlash}`,
    });
    if (wm.workerType) table.worker_type = wm.workerType;
    if (wm.maxTaskConcurrency != null && wm.maxTaskConcurrency >= 0) table.max_task_concurrency = wm.maxTaskConcurrency;
  } else if (wm.type === "symphony") {
    table.service_name = wm.serviceName || "";
  }

  return TOML.Section(table);
}

// Builds the full scheduler config.toml as a string via @ltd/j-toml. `addr` supplies the
// privateIp/publicIp/subnetId/securityGroupId/nameSuffix values, which differ between the
// download template (placeholders) and the actual EC2 user-data script (bash variables).
function buildSchedulerConfigToml(cfg, addr) {
  var proto = cfg.transport || "ws";
  var sp = cfg.schedulerPort;
  var op = cfg.objectStoragePort;
  var wsSlash = proto === "ws" ? "/" : "";
  var ctx = Object.assign({ proto: proto, sp: sp, op: op, wsSlash: wsSlash }, addr);

  var policyEngineType = cfg.policy || "simple";
  var policySection = { policy_engine_type: policyEngineType };
  if (policyEngineType === "waterfall_v1" && cfg.workerManagers && cfg.workerManagers.length > 0) {
    var policyLines = cfg.workerManagers.map(function (wm, idx) {
      return (idx + 1) + "," + wm.id;
    }).join("\n");
    policySection.policy_content = TOML.multiline.basic(policyLines + "\n");
  }

  var root = {
    object_storage_server: TOML.Section({
      bind_address: `${proto}://0.0.0.0:${op}${wsSlash}`,
    }),
    scheduler: TOML.Section(Object.assign({
      bind_address: `${proto}://0.0.0.0:${sp}${wsSlash}`,
      object_storage_address: `${proto}://127.0.0.1:${op}${wsSlash}`,
      advertised_object_storage_address: `${proto}://${ctx.publicIp}:${op}${wsSlash}`,
    }, policySection)),
  };

  var workerManagers = (cfg.workerManagers || []).map(function (wm) {
    return buildWorkerManagerTable(wm, cfg, ctx);
  });
  if (workerManagers.length > 0) root.worker_manager = workerManagers;

  root.gui = TOML.Section({
    monitor_address: `${proto}://127.0.0.1:${sp + 2}${wsSlash}`,
    gui_address: "0.0.0.0:50001",
  });

  return TOML.stringify(root, {
    newline: "\n",
    integer: Number.MAX_SAFE_INTEGER,
    newlineAround: "section",
    forceInlineArraySpacing: 0,
  }).replace(/^\n+/, "");
}

function buildConfigToml(cfg) {
  return buildSchedulerConfigToml(cfg, {
    privateIp: "<PRIVATE_IP>",
    publicIp: "<PUBLIC_IP>",
    subnetId: "<SUBNET_ID>",
    securityGroupId: "<SECURITY_GROUP_ID>",
    nameSuffix: cfg.nameSuffix || "<suffix>",
  });
}

function parseConfigToml(text) {
  return TOML.parse(text, 1, "\n", false);
}

function configFromToml(toml) {
  var scheduler = toml.scheduler || {};
  var schedBind = scheduler.bind_address || "";
  var objBind = (toml.object_storage_server || {}).bind_address || "";

  function extractPort(addr) {
    var m = addr.match(/:(\d+)/);
    return m ? parseInt(m[1], 10) : null;
  }

  var proto = schedBind.slice(0, 3) === "tcp" ? "tcp" : "ws";
  var schedulerPort = extractPort(schedBind) || 6788;
  var objectStoragePort = extractPort(objBind) || 6789;

  var rawWms = toml.worker_manager || [];
  var workerManagers = rawWms.map(function(wm, idx) {
    var base = {
      _uid: idx + 1,
      id: wm.worker_manager_id || ("wm-" + (idx + 1)),
      type: wm.type || "orb_aws_ec2",
    };
    if (wm.type === "orb_aws_ec2") {
      var orbInstType = wm.instance_type;
      var orbInst = (window.SCALER_INSTANCES || []).find(function (i) { return i.type === orbInstType; });
      if (!orbInst || orbInst.vcpu == null) throw new Error("Unknown EC2 instance type: " + orbInstType);
      var orbInstanceCap = wm.max_task_concurrency != null
        ? Math.max(1, Math.round(wm.max_task_concurrency / orbInst.vcpu))
        : 4;
      return Object.assign(base, {
        instanceType: orbInstType,
        capMode: "instances",
        instanceCap: orbInstanceCap,
        budgetCap: 10,
        requirements: wm.requirements_txt || "opengris-scaler[all]",
      });
    }
    if (wm.type === "aws_raw_ecs") {
      return Object.assign(base, {
        ecsCluster: wm.ecs_cluster || "",
        ecsTaskImage: wm.ecs_task_image || "",
        ecsSubnets: wm.ecs_subnets || "",
        ecsTaskDefinition: wm.ecs_task_definition || "",
        ecsTaskCpu: wm.ecs_task_cpu || 4,
        ecsTaskMemory: wm.ecs_task_memory || 30,
        requirements: wm.ecs_python_requirements || "opengris-scaler[all]",
      });
    }
    if (wm.type === "aws_hpc") {
      return Object.assign(base, {
        jobQueue: wm.job_queue || "",
        jobDefinition: wm.job_definition || "",
        s3Bucket: wm.s3_bucket || "",
        s3Prefix: wm.s3_prefix || "scaler-tasks",
        maxConcurrentJobs: wm.max_concurrent_jobs || 100,
        jobTimeoutMinutes: wm.job_timeout_minutes || 60,
      });
    }
    if (wm.type === "oci_raw") {
      var ociOcpus = wm.instance_ocpus;
      var ociMemoryGb = wm.instance_memory_gb;
      if (ociOcpus == null) throw new Error("oci_raw config missing instance_ocpus");
      if (ociMemoryGb == null) throw new Error("oci_raw config missing instance_memory_gb");
      var ociInstanceCap = wm.max_task_concurrency != null
        ? Math.max(1, Math.round(wm.max_task_concurrency / ociOcpus))
        : 4;
      return Object.assign(base, {
        ociShape: wm.instance_shape || "CI.Standard.A1.Flex",
        ociOcpus: ociOcpus,
        ociMemoryGb: ociMemoryGb,
        ociRegion: wm.oci_region || "",
        ociCompartmentId: wm.compartment_id || "",
        ociAvailabilityDomain: wm.availability_domain || "",
        ociSubnetId: wm.subnet_id || "",
        ociContainerImage: wm.container_image || "",
        requirements: wm.requirements_txt || "",
        capMode: "instances",
        instanceCap: ociInstanceCap,
        budgetCap: 10,
      });
    }
    if (wm.type === "oci_hpc") {
      return Object.assign(base, {
        ociRegion: wm.oci_region || "",
        ociCompartmentId: wm.compartment_id || "",
        ociAvailabilityDomain: wm.availability_domain || "",
        ociSubnetId: wm.subnet_id || "",
        ociContainerImage: wm.container_image || "",
        ociObjectStorageNamespace: wm.object_storage_namespace || "",
        ociObjectStorageBucket: wm.object_storage_bucket || "",
        ociObjectStoragePrefix: wm.object_storage_prefix || "scaler-tasks",
        ociOcpus: wm.instance_ocpus || 1,
        ociMemoryGb: wm.instance_memory_gb || 6,
        ociMaxConcurrentJobs: wm.base_concurrency || 100,
        ociJobTimeoutMinutes: Math.round((wm.job_timeout_seconds || 3600) / 60),
      });
    }
    if (wm.type === "symphony") {
      return Object.assign(base, { serviceName: wm.service_name || "" });
    }
    return base;
  });

  var pyVer = "3.14";
  for (var j = 0; j < rawWms.length; j++) {
    if (rawWms[j].python_version) { pyVer = rawWms[j].python_version; break; }
  }

  var region = "us-east-1";
  for (var k = 0; k < rawWms.length; k++) {
    if (rawWms[k].aws_region) { region = rawWms[k].aws_region; break; }
  }

  var policy = scheduler.policy_engine_type || "simple";
  var policyContent = scheduler.policy_content || "";

  if (policy === "waterfall_v1" && policyContent) {
    var priorityMap = {};
    policyContent.split("\n").forEach(function(line) {
      line = line.split("#")[0].trim();
      if (!line) return;
      var parts = line.split(",");
      if (parts.length >= 2) {
        var pri = parseInt(parts[0].trim(), 10);
        var wmId = parts[1].trim();
        if (!isNaN(pri) && wmId) priorityMap[wmId] = pri;
      }
    });
    workerManagers.sort(function(a, b) {
      return (priorityMap[a.id] !== undefined ? priorityMap[a.id] : 999) -
             (priorityMap[b.id] !== undefined ? priorityMap[b.id] : 999);
    });
  }

  var networkBackend = null;
  for (var k = 0; k < rawWms.length; k++) {
    if (rawWms[k].network_backend) { networkBackend = rawWms[k].network_backend; break; }
  }


  return {
    transport: proto,
    schedulerPort: schedulerPort,
    objectStoragePort: objectStoragePort,
    pythonVersion: pyVer,
    region: region,
    workerManagers: workerManagers.length ? workerManagers : null,
    policy: policy,
    networkBackend: networkBackend,
  };
}

function buildUserData(cfg, creds) {
  var isGitInstall = cfg.scalerPackage.indexOf("git+") >= 0;

  // When installing from a git repo, the C++ extension must be compiled from source.
  // AL2023 defaults to GCC 11 (no C++23 <expected>); Cap'n Proto must be built manually.
  var gitBuildLines = "";
  var scalerInstallLine = `uv pip install '${cfg.scalerPackage}'\n`;
  if (isGitInstall) {
    // Extract the bare https:// clone URL and optional branch from the pip spec.
    // Spec form: "opengris-scaler[all] @ git+https://github.com/org/repo@branch"
    var gitPlusIdx = cfg.scalerPackage.indexOf("git+");
    var rawUrl = cfg.scalerPackage.slice(gitPlusIdx + 4); // strip "git+"
    var atIdx = rawUrl.lastIndexOf("@");
    var cloneUrl = atIdx >= 0 ? rawUrl.slice(0, atIdx) : rawUrl;
    var cloneBranch = atIdx >= 0 ? rawUrl.slice(atIdx + 1) : "";
    var cloneCmd = cloneBranch
      ? `git clone -b ${cloneBranch} --depth 1 ${cloneUrl} /opt/scaler-src`
      : `git clone --depth 1 ${cloneUrl} /opt/scaler-src`;

    gitBuildLines = `# C++ build deps: GCC 14 (required for C++23 <expected>) + Cap'n Proto toolchain
dnf install -y git gcc14 gcc14-c++ gcc14-libstdc++-devel autoconf automake libtool libuv-devel openssl-devel

# Clone repo to access build scripts
${cloneCmd}

# Build and install Cap'n Proto via the vendored build script
cd /opt/scaler-src
CC=/usr/bin/gcc14-gcc CXX=/usr/bin/gcc14-g++ bash scripts/library_tool.sh capnp download
CC=/usr/bin/gcc14-gcc CXX=/usr/bin/gcc14-g++ bash scripts/library_tool.sh capnp compile
bash scripts/library_tool.sh capnp install
cd /

# AL2023 excludes /usr/local/lib from ldconfig by default
echo '/usr/local/lib' > /etc/ld.so.conf.d/local.conf
ldconfig

`;

    // scikit-build-core spawns CMake in an isolated env — CC/CXX are not forwarded,
    // must pass via CMAKE_ARGS. Static libuv.a lacks -fPIC on AL2023; force shared via pkg-config.
    scalerInstallLine = `PKG_CONFIG_PATH=/usr/local/lib/pkgconfig \\
CMAKE_ARGS='-DCMAKE_C_COMPILER=/usr/bin/gcc14-gcc -DCMAKE_CXX_COMPILER=/usr/bin/gcc14-g++ -DCMAKE_DISABLE_FIND_PACKAGE_libuv=TRUE' \\
  uv pip install '${cfg.scalerPackage}'
`;
  }

  var configToml = buildSchedulerConfigToml(cfg, {
    privateIp: "$PRIVATE_IP",
    publicIp: "$PUBLIC_IP",
    subnetId: "$SUBNET_ID",
    securityGroupId: cfg.securityGroupId,
    nameSuffix: cfg.nameSuffix,
  });

  var ociConfigBlock = "";
  if (creds.ociUserId && creds.ociTenancyId && creds.ociFingerprint && creds.ociPrivateKey) {
    ociConfigBlock = `
mkdir -p /root/.oci
cat > /root/.oci/oci_api_key.pem << OCI_KEY_EOF
${creds.ociPrivateKey.trim()}
OCI_KEY_EOF
chmod 600 /root/.oci/oci_api_key.pem

cat > /root/.oci/config << OCI_CFG_EOF
[DEFAULT]
user=${creds.ociUserId}
fingerprint=${creds.ociFingerprint}
tenancy=${creds.ociTenancyId}
key_file=/root/.oci/oci_api_key.pem
OCI_CFG_EOF
chmod 600 /root/.oci/config
`;
  }

  // $IMDS_TOKEN / $PUBLIC_IP / $PRIVATE_IP / $SUBNET_ID / $MAC / $! are bash variables expanded
  // at runtime on the EC2 instance — JS template literals only interpolate ${...}, not $name.
  // WORKAROUND: ORB worker manager bug — does not pick up instance-profile credentials correctly,
  // so we write them explicitly to ~/.aws/config. Remove once fixed in orb-py.
  return `#!/bin/bash
set -euxo pipefail

IMDS_TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
PUBLIC_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
PRIVATE_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
MAC=$(curl -sf -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/network/interfaces/macs/ | head -1 | tr -d '/')
SUBNET_ID=$(curl -sf -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" "http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-id")

export HOME=/root
${gitBuildLines}curl -LsSf https://astral.sh/uv/install.sh | sh
source /root/.local/bin/env

uv venv --python ${cfg.pythonVersion} /opt/scaler-venv
source /opt/scaler-venv/bin/activate
${scalerInstallLine}uv pip install --upgrade orb-py

mkdir -p /root/.aws
cat > /root/.aws/config << AWS_EOF
[default]
aws_access_key_id = ${creds.accessKeyId}
aws_secret_access_key = ${creds.secretKey}
region = ${cfg.region}
AWS_EOF
chmod 600 /root/.aws/config
${ociConfigBlock}
mkdir -p /opt/scaler

cat > /opt/scaler/config.toml << CONFIG_EOF
${configToml}CONFIG_EOF

uv tool install certbot
/root/.local/bin/certbot certonly \
  --standalone \
  --non-interactive \
  --agree-tos \
  --register-unsafely-without-email \
  -d $PUBLIC_IP

SCALER_NETWORK_BACKEND=${cfg.networkBackend || "ymq"} /opt/scaler-venv/bin/scaler /opt/scaler/config.toml >> /var/log/scaler.log 2>&1 &
echo "Scaler started (PID=$!)"
`;
}

function makeAwsClients(region, creds) {
  var awsCfg = {
    region: region,
    credentials: new AWS.Credentials(creds.accessKeyId, creds.secretKey),
  };
  return { ec2: new AWS.EC2(awsCfg), iam: new AWS.IAM(awsCfg) };
}

async function getMyPublicIp() {
  var resp = await fetch("https://checkip.amazonaws.com");
  return (await resp.text()).trim();
}

async function getLatestAl2023Ami(ec2) {
  var resp = await ec2
    .describeImages({
      Owners: ["amazon"],
      Filters: [
        { Name: "name", Values: ["al2023-ami-2023.*-kernel-*-x86_64"] },
        { Name: "state", Values: ["available"] },
      ],
    })
    .promise();
  if (!resp.Images || resp.Images.length === 0)
    throw new Error("No AL2023 x86_64 AMI found in this region");
  var sorted = resp.Images.slice().sort(function (a, b) {
    return a.CreationDate.localeCompare(b.CreationDate);
  });
  return sorted[sorted.length - 1].ImageId;
}

async function waitForWs(host, port, timeoutMs, intervalMs, signal) {
  var deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (signal && signal.aborted)
      throw new DOMException("Aborted", "AbortError");
    var ok = await new Promise(function (resolve) {
      var ws = new WebSocket("ws://" + host + ":" + port + "/");
      var done = false;
      function finish(v) {
        if (!done) {
          done = true;
          resolve(v);
        }
      }
      var timer = setTimeout(function () {
        try {
          ws.close();
        } catch (_) {}
        finish(false);
      }, 5000);
      ws.onopen = function () {
        clearTimeout(timer);
        try {
          ws.close();
        } catch (_) {}
        finish(true);
      };
      ws.onerror = function () {
        clearTimeout(timer);
        finish(false);
      };
      if (signal)
        signal.addEventListener(
          "abort",
          function () {
            clearTimeout(timer);
            try {
              ws.close();
            } catch (_) {}
            finish(false);
          },
          { once: true },
        );
    });
    if (ok) return true;
    var remaining = deadline - Date.now();
    if (remaining <= 0) break;
    await sleep(Math.min(intervalMs, remaining), signal);
  }
  return false;
}

async function ignoreNotFound(fn, addLog, signal) {
  try {
    if (addLog && signal) {
      await retrying(addLog, signal, fn);
    } else {
      await fn();
    }
  } catch (e) {
    if (e.name === "RetryPausedError" || e.name === "AbortError") throw e;
    if (e.code !== "NoSuchEntity" && e.code !== "NoSuchEntityException")
      throw e;
  }
}

async function createIamStack(iam, suffix, addLog, signal) {
  var roleName = "scaler-ec2-role-" + suffix;
  var profileName = "scaler-ec2-profile-" + suffix;

  addLog("Creating IAM role '" + roleName + "'...", "cmd");
  await retrying(addLog, signal, () =>
    iam
      .createRole({
        RoleName: roleName,
        AssumeRolePolicyDocument: _EC2_TRUST_POLICY,
        Description: "OpenGRIS Scaler ORB worker manager role",
      })
      .promise(),
  );
  await retrying(addLog, signal, () =>
    iam
      .putRolePolicy({
        RoleName: roleName,
        PolicyName: "ScalerORBPolicy",
        PolicyDocument: _ORB_INLINE_POLICY,
      })
      .promise(),
  );
  await retrying(addLog, signal, () =>
    iam
      .attachRolePolicy({
        RoleName: roleName,
        PolicyArn: "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
      })
      .promise(),
  );

  addLog("Creating instance profile '" + profileName + "'...", "cmd");
  try {
    await retrying(addLog, signal, () =>
      iam.createInstanceProfile({ InstanceProfileName: profileName }).promise(),
    );
    await retrying(addLog, signal, () =>
      iam
        .addRoleToInstanceProfile({
          InstanceProfileName: profileName,
          RoleName: roleName,
        })
        .promise(),
    );
  } catch (err) {
    try {
      await iam
        .deleteRolePolicy({ RoleName: roleName, PolicyName: "ScalerORBPolicy" })
        .promise();
      await iam.deleteRole({ RoleName: roleName }).promise();
    } catch (_) {}
    throw err;
  }

  return {
    instance_profile_name: profileName,
    role_name: roleName,
    created: true,
  };
}

async function destroyIamStack(iam, iamState, addLog, signal) {
  if (!iamState || !iamState.created) return;
  var profileName = iamState.instance_profile_name;
  var roleName = iamState.role_name;

  addLog("Removing role from instance profile '" + profileName + "'...", "cmd");
  await ignoreNotFound(
    () =>
      iam
        .removeRoleFromInstanceProfile({
          InstanceProfileName: profileName,
          RoleName: roleName,
        })
        .promise(),
    addLog,
    signal,
  );

  addLog("Deleting instance profile '" + profileName + "'...", "cmd");
  await ignoreNotFound(
    () => iam.deleteInstanceProfile({ InstanceProfileName: profileName }).promise(),
    addLog,
    signal,
  );

  addLog("Deleting role '" + roleName + "'...", "cmd");
  try {
    var attachedPolicies = await retrying(addLog, signal, () =>
      iam.listAttachedRolePolicies({ RoleName: roleName }).promise(),
    );
    for (var i = 0; i < attachedPolicies.AttachedPolicies.length; i++) {
      await retrying(addLog, signal, () =>
        iam
          .detachRolePolicy({
            RoleName: roleName,
            PolicyArn: attachedPolicies.AttachedPolicies[i].PolicyArn,
          })
          .promise(),
      );
    }
    var inlinePolicies = await retrying(addLog, signal, () =>
      iam.listRolePolicies({ RoleName: roleName }).promise(),
    );
    for (var j = 0; j < inlinePolicies.PolicyNames.length; j++) {
      await retrying(addLog, signal, () =>
        iam
          .deleteRolePolicy({
            RoleName: roleName,
            PolicyName: inlinePolicies.PolicyNames[j],
          })
          .promise(),
      );
    }
    await retrying(addLog, signal, () =>
      iam.deleteRole({ RoleName: roleName }).promise(),
    );
  } catch (e) {
    if (e.name === "RetryPausedError" || e.name === "AbortError") throw e;
    if (e.code !== "NoSuchEntity" && e.code !== "NoSuchEntityException")
      throw e;
  }
}

async function provision(
  cfg,
  creds,
  addLog,
  onPartialState,
  onKeyReady,
  signal,
  resumeState,
) {
  var clients = makeAwsClients(cfg.region, creds);
  var ec2 = clients.ec2,
    iam = clients.iam;
  var suffix = resumeState ? resumeState.name_suffix : cfg.nameSuffix;
  var partial = resumeState
    ? Object.assign({}, resumeState)
    : { region: cfg.region, name_suffix: suffix };

  if (resumeState) {
    addLog("Resuming deployment from checkpoint…", "dim");
  }
  addLog("openGRIS Scaler — EC2 deployment", "dim");
  addLog("─".repeat(52), "dim");

  // 1. AMI
  addLog(
    cfg.amiId
      ? "Using AMI: " + cfg.amiId
      : "Discovering latest AL2023 x86_64 AMI...",
    "cmd",
  );
  var amiId = cfg.amiId || (await retrying(addLog, signal, () => getLatestAl2023Ami(ec2)));
  addLog("  → " + amiId, "info");

  // 2. IAM
  var iamState;
  if (partial.iam) {
    iamState = partial.iam;
    addLog("  → Checkpoint: IAM profile '" + iamState.instance_profile_name + "' already exists", "info");
  } else if (cfg.instanceProfileName) {
    iamState = {
      instance_profile_name: cfg.instanceProfileName,
      role_name: "",
      created: false,
    };
    addLog("  → Using existing instance profile: " + cfg.instanceProfileName, "info");
  } else {
    iamState = await createIamStack(iam, suffix, addLog, signal);
    addLog("  ✓ IAM role and profile created", "ok");
  }
  partial.iam = iamState;
  onPartialState(partial);

  // 3. Key pair
  var keyPairName = "scaler-key-" + suffix;
  if (partial.key_pair_name) {
    addLog("  → Checkpoint: key pair '" + keyPairName + "' already created", "info");
  } else {
    addLog("Creating key pair '" + keyPairName + "'...", "cmd");
    var keyResp = await retrying(addLog, signal, () =>
      ec2
        .createKeyPair({
          KeyName: keyPairName,
          TagSpecifications: [
            {
              ResourceType: "key-pair",
              Tags: [{ Key: "scaler-deployment", Value: suffix }],
            },
          ],
        })
        .promise(),
    );
    onKeyReady(keyPairName, keyResp.KeyMaterial);
    addLog("  ✓ Key pair ready — download via the deployment panel", "ok");
    partial.key_pair_name = keyPairName;
    partial.key_file = keyPairName + ".pem";
    onPartialState(partial);
  }

  // 4. Default VPC
  {
    addLog("Checking for default VPC...", "cmd");
    var vpcCheck = await retrying(addLog, signal, () =>
      ec2.describeVpcs({ Filters: [{ Name: "isDefault", Values: ["true"] }] }).promise(),
    );
    if (vpcCheck.Vpcs.length === 0) {
      addLog("  → No default VPC found — creating one...", "info");
      await retrying(addLog, signal, () => ec2.createDefaultVpc({}).promise());
      addLog("  ✓ Default VPC created", "ok");
    } else {
      addLog("  → Default VPC: " + vpcCheck.Vpcs[0].VpcId, "info");
    }
  }

  // 5. Security group
  var sgId;
  if (partial.security_group_id) {
    sgId = partial.security_group_id;
    addLog("  → Checkpoint: security group " + sgId + " already exists", "info");
  } else {
    var myIp = await retrying(addLog, signal, () => getMyPublicIp());
    var sgName = "scaler-sg-" + suffix;
    addLog(
      "Creating security group '" + sgName + "' (your IP: " + myIp + ")...",
      "cmd",
    );
    var sgResp = await retrying(addLog, signal, () =>
      ec2
        .createSecurityGroup({
          GroupName: sgName,
          Description: "OpenGRIS Scaler scheduler [" + suffix + "]",
          TagSpecifications: [
            {
              ResourceType: "security-group",
              Tags: [
                { Key: "Name", Value: sgName },
                { Key: "scaler-deployment", Value: suffix },
              ],
            },
          ],
        })
        .promise(),
    );
    sgId = sgResp.GroupId;
    partial.security_group_id = sgId;
    onPartialState(partial);

    await retrying(addLog, signal, () =>
      ec2
        .authorizeSecurityGroupIngress({
          GroupId: sgId,
          IpPermissions: [
            {
              IpProtocol: "tcp",
              FromPort: 22,
              ToPort: 22,
              IpRanges: [
                { CidrIp: myIp + "/32", Description: "SSH from local machine" },
              ],
            },
            {
              IpProtocol: "tcp",
              FromPort: cfg.schedulerPort,
              ToPort: cfg.schedulerPort,
              IpRanges: [
                { CidrIp: myIp + "/32", Description: "Scaler scheduler from local machine" },
              ],
            },
            {
              IpProtocol: "tcp",
              FromPort: cfg.schedulerPort + 2,
              ToPort: cfg.schedulerPort + 2,
              IpRanges: [
                { CidrIp: myIp + "/32", Description: "Scaler scheduler monitor from local machine" },
              ],
            },
            {
              IpProtocol: "tcp",
              FromPort: cfg.objectStoragePort,
              ToPort: cfg.objectStoragePort,
              IpRanges: [
                { CidrIp: myIp + "/32", Description: "Scaler object storage from local machine" },
              ],
            },
            {
              IpProtocol: "tcp",
              FromPort: 50001,
              ToPort: 50001,
              IpRanges: [
                { CidrIp: myIp + "/32", Description: "Scaler Worker Monitor from local machine" },
              ],
            },
            {
              IpProtocol: "tcp",
              FromPort: 80,
              ToPort: 80,
              IpRanges: [
                { CidrIp: "0.0.0.0/0", Description: "ACME HTTP-01 challenge (Let's Encrypt)" },
              ],
            },
          ],
        })
        .promise(),
    );
    addLog("  ✓ Security group created: " + sgId, "ok");
  }

  // 6. Launch instance
  var instanceId;
  if (partial.instance_id) {
    instanceId = partial.instance_id;
    addLog("  → Checkpoint: instance " + instanceId + " already launched", "info");
  } else {
    cfg = Object.assign({}, cfg, { securityGroupId: sgId });
    var userData = buildUserData(cfg, creds);
    addLog("Launching " + cfg.instanceType + " instance...", "cmd");
    var runParams = {
      ImageId: amiId,
      InstanceType: cfg.instanceType,
      KeyName: keyPairName,
      SecurityGroupIds: [sgId],
      IamInstanceProfile: { Name: iamState.instance_profile_name },
      UserData: btoa(userData),
      MinCount: 1,
      MaxCount: 1,
      TagSpecifications: [
        {
          ResourceType: "instance",
          Tags: [
            { Key: "Name", Value: "scaler-scheduler-" + suffix },
            { Key: "scaler-deployment", Value: suffix },
          ],
        },
      ],
    };
    var runResp;
    var iamDeadline = Date.now() + 60000;
    while (true) {
      try {
        runResp = await withAbort(ec2.runInstances(runParams).promise(), signal);
        break;
      } catch (err) {
        var iamNotReady =
          err.code === "InvalidParameterValue" &&
          err.message &&
          err.message.includes("Invalid IAM Instance Profile");
        if (!iamNotReady || Date.now() >= iamDeadline) {
          if (!iamNotReady) {
            // Non-IAM error — use normal retry/pause logic
            var paused = new Error(err.message);
            paused.name = "RetryPausedError";
            throw paused;
          }
          throw err;
        }
        addLog("  → IAM profile not yet visible to EC2, retrying in 5s…", "dim");
        await sleep(5000, signal);
      }
    }
    instanceId = runResp.Instances[0].InstanceId;
    partial.instance_id = instanceId;
    onPartialState(partial);
    addLog("  → Instance launched: " + instanceId, "info");
  }

  // 7. Wait for running state + post-launch network rules
  var publicIp, privateIp, vpcId, subnetId;
  if (partial.public_ip) {
    publicIp = partial.public_ip;
    privateIp = partial.private_ip;
    vpcId = partial.vpc_id;
    subnetId = partial.subnet_id;
    addLog("  → Checkpoint: instance running at " + publicIp, "info");
  } else {
    addLog("Waiting for instance to reach running state...", "cmd");
    await retrying(addLog, signal, () =>
      ec2.waitFor("instanceRunning", { InstanceIds: [instanceId] }).promise(),
    );

    var desc = await retrying(addLog, signal, () =>
      ec2.describeInstances({ InstanceIds: [instanceId] }).promise(),
    );
    var inst = desc.Reservations[0].Instances[0];
    publicIp = inst.PublicIpAddress;
    privateIp = inst.PrivateIpAddress;
    vpcId = inst.VpcId;
    subnetId = inst.SubnetId;
    partial.public_ip = publicIp;
    partial.private_ip = privateIp;
    partial.vpc_id = vpcId;
    partial.subnet_id = subnetId;
    partial.worker_monitor_address = "http://" + publicIp + ":50001";
    addLog("  ✓ Instance running", "ok");
    addLog("  → Public IP:  " + publicIp, "info");
    addLog("  → Private IP: " + privateIp, "info");

    // Allow all inbound traffic from the VPC's CIDR so ORB workers can reach the scheduler.
    var vpcDesc = await retrying(addLog, signal, () =>
      ec2.describeVpcs({ VpcIds: [vpcId] }).promise(),
    );
    var vpcCidr = vpcDesc.Vpcs[0].CidrBlock;
    try {
      await retrying(addLog, signal, () =>
        ec2
          .authorizeSecurityGroupIngress({
            GroupId: sgId,
            IpPermissions: [
              {
                IpProtocol: "-1",
                IpRanges: [
                  { CidrIp: vpcCidr, Description: "All traffic from VPC (ORB workers)" },
                ],
              },
            ],
          })
          .promise(),
      );
    } catch (e) {
      if (e.name === "RetryPausedError" || e.name === "AbortError") throw e;
    }
    addLog("  → VPC: " + vpcId + "  CIDR: " + vpcCidr + "  subnet: " + subnetId, "info");

    // OCI workers connect over the public internet — open scheduler and object storage ports.
    var hasOci = (cfg.workerManagers || []).some(function (wm) {
      return wm.type === "oci_raw" || wm.type === "oci_hpc";
    });
    if (hasOci) {
      try {
        await retrying(addLog, signal, () =>
          ec2
            .authorizeSecurityGroupIngress({
              GroupId: sgId,
              IpPermissions: [
                {
                  IpProtocol: "tcp",
                  FromPort: cfg.schedulerPort,
                  ToPort: cfg.schedulerPort,
                  IpRanges: [{ CidrIp: "0.0.0.0/0", Description: "Scheduler (OCI workers)" }],
                },
                {
                  IpProtocol: "tcp",
                  FromPort: cfg.objectStoragePort,
                  ToPort: cfg.objectStoragePort,
                  IpRanges: [{ CidrIp: "0.0.0.0/0", Description: "Object storage (OCI workers)" }],
                },
              ],
            })
            .promise(),
        );
      } catch (e) {
        if (e.name === "RetryPausedError" || e.name === "AbortError") throw e;
      }
      addLog("  → Opened scheduler + object storage ports to 0.0.0.0/0 for OCI workers", "info");
    }
    onPartialState(partial);
  }

  // 8. Build addresses and persist complete state
  var addrSlash = cfg.transport === "ws" ? "/" : "";
  var schedAddr =
    cfg.transport + "://" + publicIp + ":" + cfg.schedulerPort + addrSlash;
  var objAddr =
    cfg.transport + "://" + publicIp + ":" + cfg.objectStoragePort + addrSlash;

  var state = {
    region: cfg.region,
    name_suffix: suffix,
    instance_id: instanceId,
    key_pair_name: keyPairName,
    key_file: keyPairName + ".pem",
    security_group_id: sgId,
    public_ip: publicIp,
    private_ip: privateIp,
    vpc_id: vpcId,
    subnet_id: subnetId,
    transport: cfg.transport,
    scheduler_port: cfg.schedulerPort,
    object_storage_port: cfg.objectStoragePort,
    scheduler_address: schedAddr,
    object_storage_address: objAddr,
    monitor_address:
      cfg.transport +
      "://" +
      publicIp +
      ":" +
      (cfg.schedulerPort + 2) +
      addrSlash,
    worker_monitor_address: "http://" + publicIp + ":50001",
    iam: iamState,
    worker_name: "scaler-worker-" + suffix,
  };
  onPartialState(state);

  // 9. Poll for scheduler readiness — temporarily skipped: browsers block ws:// from https pages (mixed content).
  addLog(
    "  ℹ Skipping scheduler connection check (browser security restriction) — assuming ready",
    "warn",
  );
  // if (cfg.transport === "ws") {
  //   addLog(
  //     "Waiting up to " + cfg.pollTimeout + "s for scheduler at " + publicIp + ":" + cfg.schedulerPort + "...",
  //     "cmd",
  //   );
  //   var ready = await waitForWs(publicIp, cfg.schedulerPort, cfg.pollTimeout * 1000, cfg.pollInterval * 1000, signal);
  //   if (ready) {
  //     addLog("  ✓ Scheduler is reachable", "ok");
  //   } else {
  //     addLog("  ✗ Could not verify readiness — check /var/log/scaler.log on the instance", "warn");
  //   }
  // } else {
  //   addLog("  ℹ Skipping scheduler connection check — browsers can't open TCP connections", "warn");
  // }

  addLog("─".repeat(52), "dim");
  addLog("  DEPLOYMENT COMPLETE", "done");
  addLog("─".repeat(52), "dim");

  return state;
}

async function teardown(state, creds, addLog, signal) {
  var clients = makeAwsClients(state.region, creds);
  var ec2 = clients.ec2,
    iam = clients.iam;

  addLog("openGRIS Scaler — teardown", "dim");
  addLog("─".repeat(52), "dim");

  // Terminate all deployment instances (scheduler + any ORB workers) in one pass.
  // Search by scaler-deployment tag; fall back to explicit instance_id for old state files.
  addLog(
    "Searching for deployment instances (scaler-deployment=" +
      state.name_suffix +
      ")...",
    "cmd",
  );
  var instanceIdSet = {};
  if (state.name_suffix) {
    var tagResp = await retrying(addLog, signal, () =>
      ec2
        .describeInstances({
          Filters: [
            { Name: "tag:scaler-deployment", Values: [state.name_suffix] },
            {
              Name: "instance-state-name",
              Values: ["pending", "running", "stopping", "stopped"],
            },
          ],
        })
        .promise(),
    );
    tagResp.Reservations.forEach(function (r) {
      r.Instances.forEach(function (i) {
        instanceIdSet[i.InstanceId] = true;
      });
    });
  }
  if (state.instance_id) instanceIdSet[state.instance_id] = true;
  var allInstanceIds = Object.keys(instanceIdSet);
  if (allInstanceIds.length > 0) {
    addLog(
      "  → Terminating " +
        allInstanceIds.length +
        " instance(s): " +
        allInstanceIds.join(", "),
      "info",
    );
    await retrying(addLog, signal, () =>
      ec2.terminateInstances({ InstanceIds: allInstanceIds }).promise(),
    );
    await retrying(addLog, signal, () =>
      ec2
        .waitFor("instanceTerminated", { InstanceIds: allInstanceIds })
        .promise(),
    );
    addLog("  ✓ All instances terminated", "ok");
  } else {
    addLog("  → No live instances found", "info");
  }

  if (state.security_group_id) {
    addLog("Deleting security group " + state.security_group_id + "...", "cmd");
    await retrying(addLog, signal, () =>
      ec2.deleteSecurityGroup({ GroupId: state.security_group_id }).promise(),
    );
    addLog("  ✓ Security group deleted", "ok");
  }

  if (state.key_pair_name) {
    addLog("Deleting key pair '" + state.key_pair_name + "'...", "cmd");
    await retrying(addLog, signal, () =>
      ec2.deleteKeyPair({ KeyName: state.key_pair_name }).promise(),
    );
    addLog("  ✓ Key pair deleted", "ok");
  }

  if (state.iam) {
    await destroyIamStack(iam, state.iam, addLog, signal);
    addLog("  ✓ IAM resources cleaned up", "ok");
  }

  addLog("─".repeat(52), "dim");
  addLog("  TEARDOWN COMPLETE", "done");
  addLog("─".repeat(52), "dim");
}
