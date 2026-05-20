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
      Action: ["ec2:*", "autoscaling:*", "iam:GetRole", "iam:PassRole", "ssm:GetParameter", "sts:GetCallerIdentity"],
      Resource: "*",
    },
  ],
});

function randomSuffix(n) {
  n = n || 8;
  var chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from({ length: n }, function () {
    return chars[Math.floor(Math.random() * chars.length)];
  }).join("");
}

function sleep(ms, signal) {
  return new Promise(function (resolve, reject) {
    if (signal && signal.aborted) return reject(new DOMException("Aborted", "AbortError"));
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
      if (signal.aborted) return reject(new DOMException("Aborted", "AbortError"));
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

function buildConfigToml(cfg) {
  var proto = cfg.transport || "ws";
  var sp = cfg.schedulerPort;
  var op = cfg.objectStoragePort;
  var wsSlash = proto === "ws" ? "/" : "";
  var suffix = cfg.nameSuffix || "<suffix>";

  var wmToml = (cfg.workerManagers || [])
    .map(function (wm) {
      var block = `[[worker_manager]]
type = "${wm.type}"
scheduler_address = "${proto}://127.0.0.1:${sp}${wsSlash}"
worker_manager_id = "${wm.id}"
`;

      if (wm.type === "orb_aws_ec2") {
        var req = (wm.requirements || "").trim();
        block += `worker_scheduler_address = "${proto}://<PRIVATE_IP>:${sp}${wsSlash}"
object_storage_address = "${proto}://<PRIVATE_IP>:${op}${wsSlash}"
python_version = "${cfg.pythonVersion}"
requirements_txt = """
${req}
"""
instance_type = "${wm.instanceType}"
aws_region = "${cfg.region}"
key_name = "scaler-key-${suffix}"
subnet_id = "<SUBNET_ID>"
security_group_ids = ["<SECURITY_GROUP_ID>"]
logging_level = "INFO"
instance_tags = {scaler-deployment = "${suffix}"}
`;
        if (cfg.networkBackend !== "zmq") block += `network_backend = "${cfg.networkBackend || "ymq"}"\n`;
      } else if (wm.type === "aws_raw_ecs") {
        block += `aws_region = "${cfg.region}"
ecs_cluster = "${wm.ecsCluster || "scaler-cluster"}"
ecs_task_image = "${wm.ecsTaskImage || ""}"
ecs_subnets = "${wm.ecsSubnets || ""}"
ecs_task_definition = "${wm.ecsTaskDefinition || "scaler-task-definition"}"
ecs_task_cpu = ${wm.ecsTaskCpu || 4}
ecs_task_memory = ${wm.ecsTaskMemory || 30}
ecs_python_version = "${cfg.pythonVersion}"
`;
        if (wm.requirements) block += `ecs_python_requirements = "${wm.requirements}"\n`;
      } else if (wm.type === "aws_hpc") {
        block += `aws_region = "${cfg.region}"
job_queue = "${wm.jobQueue || ""}"
job_definition = "${wm.jobDefinition || ""}"
s3_bucket = "${wm.s3Bucket || ""}"
s3_prefix = "${wm.s3Prefix || "scaler-tasks"}"
max_concurrent_jobs = ${wm.maxConcurrentJobs || 100}
job_timeout_minutes = ${wm.jobTimeoutMinutes || 60}
`;
      } else if (wm.type === "baremetal_native") {
        block += `mode = "${wm.mode || "fixed"}"
object_storage_address = "${proto}://127.0.0.1:${op}${wsSlash}"
`;
        if (wm.workerType) block += `worker_type = "${wm.workerType}"\n`;
        if (wm.maxTaskConcurrency != null && wm.maxTaskConcurrency >= 0)
          block += `max_task_concurrency = ${wm.maxTaskConcurrency}\n`;
      } else if (wm.type === "symphony") {
        block += `service_name = "${wm.serviceName || ""}"\n`;
      }

      return block;
    })
    .join("\n");

  return `[object_storage_server]
bind_address = "${proto}://0.0.0.0:${op}${wsSlash}"

[scheduler]
bind_address = "${proto}://0.0.0.0:${sp}${wsSlash}"
object_storage_address = "${proto}://127.0.0.1:${op}${wsSlash}"
advertised_object_storage_address = "${proto}://<PUBLIC_IP>:${op}${wsSlash}"

${wmToml}
[gui]
monitor_address = "${proto}://127.0.0.1:${sp + 2}${wsSlash}"
gui_address = "0.0.0.0:50001"
`;
}

function buildUserData(cfg, creds) {
  var proto = cfg.transport || "ws";
  var sp = cfg.schedulerPort;
  var op = cfg.objectStoragePort;
  var wsSlash = proto === "ws" ? "/" : "";

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
dnf install -y git gcc14 gcc14-c++ gcc14-libstdc++-devel autoconf automake libtool libuv-devel

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

  // Build one [[worker_manager]] TOML block per configured manager.
  var wmToml = (cfg.workerManagers || [])
    .map(function (wm) {
      var block = `[[worker_manager]]
type = "${wm.type}"
scheduler_address = "${proto}://127.0.0.1:${sp}${wsSlash}"
worker_manager_id = "${wm.id}"
`;

      if (wm.type === "orb_aws_ec2") {
        var req = (wm.requirements || "").trim();
        block += `worker_scheduler_address = "${proto}://$PRIVATE_IP:${sp}${wsSlash}"
object_storage_address = "${proto}://$PRIVATE_IP:${op}${wsSlash}"
python_version = "${cfg.pythonVersion}"
requirements_txt = """
${req}
"""
instance_type = "${wm.instanceType}"
aws_region = "${cfg.region}"
key_name = "scaler-key-${cfg.nameSuffix}"
subnet_id = "$SUBNET_ID"
security_group_ids = ["${cfg.securityGroupId}"]
logging_level = "INFO"
instance_tags = {scaler-deployment = "${cfg.nameSuffix}"}
`;
        if (cfg.networkBackend !== "zmq") block += `network_backend = "${cfg.networkBackend || "ymq"}"\n`;
      } else if (wm.type === "aws_raw_ecs") {
        block += `aws_region = "${cfg.region}"
ecs_cluster = "${wm.ecsCluster || "scaler-cluster"}"
ecs_task_image = "${wm.ecsTaskImage || ""}"
ecs_subnets = "${wm.ecsSubnets || ""}"
ecs_task_definition = "${wm.ecsTaskDefinition || "scaler-task-definition"}"
ecs_task_cpu = ${wm.ecsTaskCpu || 4}
ecs_task_memory = ${wm.ecsTaskMemory || 30}
ecs_python_version = "${cfg.pythonVersion}"
`;
        if (wm.requirements) block += `ecs_python_requirements = "${wm.requirements}"\n`;
      } else if (wm.type === "aws_hpc") {
        block += `aws_region = "${cfg.region}"
job_queue = "${wm.jobQueue || ""}"
job_definition = "${wm.jobDefinition || ""}"
s3_bucket = "${wm.s3Bucket || ""}"
s3_prefix = "${wm.s3Prefix || "scaler-tasks"}"
max_concurrent_jobs = ${wm.maxConcurrentJobs || 100}
job_timeout_minutes = ${wm.jobTimeoutMinutes || 60}
`;
      } else if (wm.type === "baremetal_native") {
        block += `mode = "${wm.mode || "fixed"}"
object_storage_address = "${proto}://127.0.0.1:${op}${wsSlash}"
`;
        if (wm.workerType) block += `worker_type = "${wm.workerType}"\n`;
        if (wm.maxTaskConcurrency != null && wm.maxTaskConcurrency >= 0)
          block += `max_task_concurrency = ${wm.maxTaskConcurrency}\n`;
      } else if (wm.type === "symphony") {
        block += `service_name = "${wm.serviceName || ""}"\n`;
      }

      return block;
    })
    .join("\n");

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

mkdir -p /opt/scaler

cat > /opt/scaler/config.toml << CONFIG_EOF
[object_storage_server]
bind_address = "${proto}://0.0.0.0:${op}${wsSlash}"

[scheduler]
bind_address = "${proto}://0.0.0.0:${sp}${wsSlash}"
object_storage_address = "${proto}://127.0.0.1:${op}${wsSlash}"
advertised_object_storage_address = "${proto}://$PUBLIC_IP:${op}${wsSlash}"

${wmToml}
[gui]
monitor_address = "${proto}://127.0.0.1:${sp + 2}${wsSlash}"
gui_address = "0.0.0.0:50001"
CONFIG_EOF

${cfg.networkBackend === "zmq" ? "SCALER_NETWORK_BACKEND=tcp_zmq " : ""}/opt/scaler-venv/bin/scaler /opt/scaler/config.toml >> /var/log/scaler.log 2>&1 &
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
  if (!resp.Images || resp.Images.length === 0) throw new Error("No AL2023 x86_64 AMI found in this region");
  var sorted = resp.Images.slice().sort(function (a, b) {
    return a.CreationDate.localeCompare(b.CreationDate);
  });
  return sorted[sorted.length - 1].ImageId;
}

async function waitForWs(host, port, timeoutMs, intervalMs, signal) {
  var deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (signal && signal.aborted) throw new DOMException("Aborted", "AbortError");
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

async function ignoreNotFound(fn) {
  try {
    await fn();
  } catch (e) {
    if (e.code !== "NoSuchEntity" && e.code !== "NoSuchEntityException") throw e;
  }
}

async function createIamStack(iam, suffix, addLog, signal) {
  var roleName = "scaler-ec2-role-" + suffix;
  var profileName = "scaler-ec2-profile-" + suffix;

  addLog("Creating IAM role '" + roleName + "'...", "cmd");
  await withAbort(
    iam
      .createRole({
        RoleName: roleName,
        AssumeRolePolicyDocument: _EC2_TRUST_POLICY,
        Description: "OpenGRIS Scaler ORB worker manager role",
      })
      .promise(),
    signal,
  );
  await withAbort(
    iam
      .putRolePolicy({
        RoleName: roleName,
        PolicyName: "ScalerORBPolicy",
        PolicyDocument: _ORB_INLINE_POLICY,
      })
      .promise(),
    signal,
  );
  await withAbort(
    iam
      .attachRolePolicy({
        RoleName: roleName,
        PolicyArn: "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
      })
      .promise(),
    signal,
  );

  addLog("Creating instance profile '" + profileName + "'...", "cmd");
  try {
    await withAbort(iam.createInstanceProfile({ InstanceProfileName: profileName }).promise(), signal);
    await withAbort(
      iam
        .addRoleToInstanceProfile({
          InstanceProfileName: profileName,
          RoleName: roleName,
        })
        .promise(),
      signal,
    );
  } catch (err) {
    try {
      await iam.deleteRolePolicy({ RoleName: roleName, PolicyName: "ScalerORBPolicy" }).promise();
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

async function destroyIamStack(iam, iamState, addLog) {
  if (!iamState || !iamState.created) return;
  var profileName = iamState.instance_profile_name;
  var roleName = iamState.role_name;

  addLog("Removing role from instance profile '" + profileName + "'...", "cmd");
  await ignoreNotFound(function () {
    return iam
      .removeRoleFromInstanceProfile({
        InstanceProfileName: profileName,
        RoleName: roleName,
      })
      .promise();
  });

  addLog("Deleting instance profile '" + profileName + "'...", "cmd");
  await ignoreNotFound(function () {
    return iam.deleteInstanceProfile({ InstanceProfileName: profileName }).promise();
  });

  addLog("Deleting role '" + roleName + "'...", "cmd");
  try {
    var attachedPolicies = await iam.listAttachedRolePolicies({ RoleName: roleName }).promise();
    for (var i = 0; i < attachedPolicies.AttachedPolicies.length; i++) {
      await iam
        .detachRolePolicy({
          RoleName: roleName,
          PolicyArn: attachedPolicies.AttachedPolicies[i].PolicyArn,
        })
        .promise();
    }
    var inlinePolicies = await iam.listRolePolicies({ RoleName: roleName }).promise();
    for (var j = 0; j < inlinePolicies.PolicyNames.length; j++) {
      await iam
        .deleteRolePolicy({
          RoleName: roleName,
          PolicyName: inlinePolicies.PolicyNames[j],
        })
        .promise();
    }
    await iam.deleteRole({ RoleName: roleName }).promise();
  } catch (e) {
    if (e.code !== "NoSuchEntity" && e.code !== "NoSuchEntityException") throw e;
  }
}

async function provision(cfg, creds, addLog, onPartialState, onKeyReady, signal) {
  var clients = makeAwsClients(cfg.region, creds);
  var ec2 = clients.ec2,
    iam = clients.iam;
  var suffix = cfg.nameSuffix;
  var partial = { region: cfg.region, name_suffix: suffix };

  addLog("openGRIS Scaler — EC2 deployment", "dim");
  addLog("─".repeat(52), "dim");

  // 1. AMI
  addLog(cfg.amiId ? "Using AMI: " + cfg.amiId : "Discovering latest AL2023 x86_64 AMI...", "cmd");
  var amiId = cfg.amiId || (await withAbort(getLatestAl2023Ami(ec2), signal));
  addLog("  → " + amiId, "info");

  // 2. IAM
  var iamState;
  if (cfg.instanceProfileName) {
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
  addLog("Creating key pair '" + keyPairName + "'...", "cmd");
  var keyResp = await withAbort(
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
    signal,
  );
  onKeyReady(keyPairName, keyResp.KeyMaterial);
  addLog("  ✓ Key pair ready — download via the deployment panel", "ok");
  partial.key_pair_name = keyPairName;
  partial.key_file = keyPairName + ".pem";
  onPartialState(partial);

  // 4. Security group
  var myIp = await withAbort(getMyPublicIp(), signal);
  var sgName = "scaler-sg-" + suffix;
  addLog("Creating security group '" + sgName + "' (your IP: " + myIp + ")...", "cmd");
  var sgResp = await withAbort(
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
    signal,
  );
  var sgId = sgResp.GroupId;
  partial.security_group_id = sgId;
  onPartialState(partial);

  await withAbort(
    ec2
      .authorizeSecurityGroupIngress({
        GroupId: sgId,
        IpPermissions: [
          {
            IpProtocol: "tcp",
            FromPort: 22,
            ToPort: 22,
            IpRanges: [{ CidrIp: myIp + "/32", Description: "SSH from local machine" }],
          },
          {
            IpProtocol: "tcp",
            FromPort: cfg.schedulerPort,
            ToPort: cfg.schedulerPort,
            IpRanges: [
              {
                CidrIp: myIp + "/32",
                Description: "Scaler scheduler from local machine",
              },
            ],
          },
          {
            IpProtocol: "tcp",
            FromPort: cfg.schedulerPort + 2,
            ToPort: cfg.schedulerPort + 2,
            IpRanges: [
              {
                CidrIp: myIp + "/32",
                Description: "Scaler scheduler monitor from local machine",
              },
            ],
          },
          {
            IpProtocol: "tcp",
            FromPort: cfg.objectStoragePort,
            ToPort: cfg.objectStoragePort,
            IpRanges: [
              {
                CidrIp: myIp + "/32",
                Description: "Scaler object storage from local machine",
              },
            ],
          },
          {
            IpProtocol: "tcp",
            FromPort: 50001,
            ToPort: 50001,
            IpRanges: [
              {
                CidrIp: myIp + "/32",
                Description: "Scaler web GUI from local machine",
              },
            ],
          },
        ],
      })
      .promise(),
    signal,
  );
  addLog("  ✓ Security group created: " + sgId, "ok");

  // 5. Launch instance
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
        err.code === "InvalidParameterValue" && err.message && err.message.includes("Invalid IAM Instance Profile");
      if (!iamNotReady || Date.now() >= iamDeadline) throw err;
      addLog("  → IAM profile not yet visible to EC2, retrying in 5s…", "dim");
      await sleep(5000, signal);
    }
  }
  var instanceId = runResp.Instances[0].InstanceId;
  partial.instance_id = instanceId;
  onPartialState(partial);
  addLog("  → Instance launched: " + instanceId, "info");

  // 6. Wait for running state
  addLog("Waiting for instance to reach running state...", "cmd");
  await withAbort(ec2.waitFor("instanceRunning", { InstanceIds: [instanceId] }).promise(), signal);

  var desc = await withAbort(ec2.describeInstances({ InstanceIds: [instanceId] }).promise(), signal);
  var inst = desc.Reservations[0].Instances[0];
  var publicIp = inst.PublicIpAddress;
  var privateIp = inst.PrivateIpAddress;
  var vpcId = inst.VpcId;
  var subnetId = inst.SubnetId;
  partial.public_ip = publicIp;
  partial.private_ip = privateIp;
  partial.vpc_id = vpcId;
  partial.subnet_id = subnetId;
  partial.gui_address = "http://" + publicIp + ":50001";
  addLog("  ✓ Instance running", "ok");
  addLog("  → Public IP:  " + publicIp, "info");
  addLog("  → Private IP: " + privateIp, "info");

  // Allow all inbound traffic from the VPC's CIDR so ORB workers can reach the scheduler.
  var vpcDesc = await withAbort(ec2.describeVpcs({ VpcIds: [vpcId] }).promise(), signal);
  var vpcCidr = vpcDesc.Vpcs[0].CidrBlock;
  await withAbort(
    ec2
      .authorizeSecurityGroupIngress({
        GroupId: sgId,
        IpPermissions: [
          {
            IpProtocol: "-1",
            IpRanges: [
              {
                CidrIp: vpcCidr,
                Description: "All traffic from VPC (ORB workers)",
              },
            ],
          },
        ],
      })
      .promise(),
    signal,
  );
  addLog("  → VPC: " + vpcId + "  CIDR: " + vpcCidr + "  subnet: " + subnetId, "info");
  onPartialState(partial);

  // 7. Build addresses and persist complete state
  var addrSlash = cfg.transport === "ws" ? "/" : "";
  var schedAddr = cfg.transport + "://" + publicIp + ":" + cfg.schedulerPort + addrSlash;
  var objAddr = cfg.transport + "://" + publicIp + ":" + cfg.objectStoragePort + addrSlash;

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
    monitor_address: cfg.transport + "://" + publicIp + ":" + (cfg.schedulerPort + 2) + addrSlash,
    gui_address: "http://" + publicIp + ":50001",
    iam: iamState,
    worker_name: "scaler-worker-" + suffix,
  };
  onPartialState(state);

  // 8. Poll for scheduler readiness — temporarily skipped: browsers block ws:// from https pages (mixed content).
  addLog("  ℹ Skipping scheduler connection check (browser security restriction) — assuming ready", "warn");
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
  addLog("Searching for deployment instances (scaler-deployment=" + state.name_suffix + ")...", "cmd");
  var instanceIdSet = {};
  if (state.name_suffix) {
    var tagResp = await withAbort(
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
      signal,
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
    addLog("  → Terminating " + allInstanceIds.length + " instance(s): " + allInstanceIds.join(", "), "info");
    try {
      await withAbort(ec2.terminateInstances({ InstanceIds: allInstanceIds }).promise(), signal);
      await withAbort(ec2.waitFor("instanceTerminated", { InstanceIds: allInstanceIds }).promise(), signal);
      addLog("  ✓ All instances terminated", "ok");
    } catch (e) {
      if (e.name === "AbortError") throw e;
      addLog("  ! Warning: " + e.message, "warn");
    }
  } else {
    addLog("  → No live instances found", "info");
  }

  if (state.security_group_id) {
    addLog("Deleting security group " + state.security_group_id + "...", "cmd");
    try {
      await withAbort(ec2.deleteSecurityGroup({ GroupId: state.security_group_id }).promise(), signal);
      addLog("  ✓ Security group deleted", "ok");
    } catch (e) {
      if (e.name === "AbortError") throw e;
      addLog("  ! Warning: " + e.message, "warn");
    }
  }

  if (state.key_pair_name) {
    addLog("Deleting key pair '" + state.key_pair_name + "'...", "cmd");
    try {
      await withAbort(ec2.deleteKeyPair({ KeyName: state.key_pair_name }).promise(), signal);
      addLog("  ✓ Key pair deleted", "ok");
    } catch (e) {
      if (e.name === "AbortError") throw e;
      addLog("  ! Warning: " + e.message, "warn");
    }
  }

  if (state.iam) {
    await destroyIamStack(iam, state.iam, addLog);
    addLog("  ✓ IAM resources cleaned up", "ok");
  }

  addLog("─".repeat(52), "dim");
  addLog("  TEARDOWN COMPLETE", "done");
  addLog("─".repeat(52), "dim");
}
