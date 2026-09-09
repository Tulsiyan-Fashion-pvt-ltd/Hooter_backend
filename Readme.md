#   Hooter

## Run the server

To run the server, you must install the docker in your system.

## Install in linux 

Below instruction is to downlod the docker in the fedora and debian destributions.

### Install in fedora

**1. Add Docker's official repository**

```
sudo dnf -y install dnf-plugins-core
sudo dnf config-manager addrepo https://download.docker.com/linux/fedora/docker-ce.repo
```

**2. Install Docker Engine**

```
sudo dnf install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

**3. Enable and start Docker**

```
sudo systemctl enable --now docker
```

**4. Allow running Docker without sudo**
```
sudo usermod -aG docker $USER
```

> Log out and log back in, then verify:

``` docker --version
docker compose version
docker run hello-world
```

---


### Install in debian

**1. Update the package index**

```bash
sudo apt update
sudo apt install -y ca-certificates curl
```

**2. Add Docker's GPG key**

```bash
sudo install -m 0755 -d /etc/apt/keyrings

sudo curl -fsSL https://download.docker.com/linux/debian/gpg \
  -o /etc/apt/keyrings/docker.asc

sudo chmod a+r /etc/apt/keyrings/docker.asc
```

**3. Add the Docker repository**

```bash
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

**4. Install Docker Engine**

```bash
sudo apt update

sudo apt install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-buildx-plugin \
  docker-compose-plugin
```

**5. Enable and start Docker**

```bash
sudo systemctl enable --now docker
```

**6. Allow Docker without `sudo` (Optional)**

```bash
sudo usermod -aG docker $USER
```

> **Note:** Log out and log back in for the group change to take effect.

**7. Verify the installation**

```bash
docker --version
docker compose version
docker run hello-world
```




## Install Docker on Windows (Official Docker Desktop)

Follow these steps to install Docker Desktop on Windows.

**1. Download Docker Desktop**

Download the installer from Docker's official website and run it.

**2. Run the installer**

- Accept the license agreement.
- Keep **Use WSL 2 instead of Hyper-V** enabled (recommended).
- Complete the installation.

**3. Restart your computer**

Restart Windows if the installer prompts you to do so.

**4. Launch Docker Desktop**

Open **Docker Desktop** from the Start Menu and wait until it shows **Engine running**.

**5. Verify the installation**

Open **PowerShell** or **Command Prompt** and run:

```bash
docker --version
docker compose version
docker run hello-world
```

> **Note:** Windows 10/11 with **WSL 2** is the recommended setup for Docker Desktop.

___

### After installing the docker. for the first time.
```
docker compose up --build
```

Once you have build the images and created the containers for all the services inside the compose.yml file, you do not have to use the flag `--build` in the `docker compose up` command. 

#### When to run the `--build` flag?
When you make the changes inside the Dockerfile or in the compose file then you build the images again.


## Run migrate
The migration happens automatically using flyway when the docker compose starts using 

> `docker compose up --build` 

If you are building after a change in the compose file or DOCKER file
Or
> `docker compose up` 

When you don't need to build the compose file

In case you get a `baseline error`, then use the baseline command inside the container

```
docker compose run --rm flyway baseline
```

Then later:

```
docker compose run --rm flyway migrate
docker compose run --rm flyway info
docker compose run --rm flyway validate
```