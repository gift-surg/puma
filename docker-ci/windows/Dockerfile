#TODO: Temp renaming of image stage?
FROM mcr.microsoft.com/powershell:6.2.3-windowsservercore-1903 as PUMA_CI

ENV chocolateyVersion='0.10.13'
ENV chocolateyUseWindowsCompression=false
RUN @powershell -NoProfile -ExecutionPolicy Bypass -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))"

RUN choco config set cachelocation C:\chococache

RUN choco install git --version 2.23.0 --confirm --params="/NoCredentialManager " \
    && choco install miniconda3 --version 4.6.14 --confirm --params="/InstallationType:AllUsers /RegisterPython:1 /S /AddToPath:1" \
    && choco install vim --version 8.0.604 --confirm \
    && rmdir /S /Q C:\chococache

# Ensure Git bash utilities come first on the PATH
RUN @powershell -Command "[Environment]::SetEnvironmentVariable('Path', 'C:\Program Files\Git\mingw64\bin;C:\Program Files\Git\usr\bin;' + $env:Path, [EnvironmentVariableTarget]::Machine)"

# Ensure SSL certificates are installed / up-to-date
RUN certutil -generateSSTFromWU roots.sst && certutil -addstore -f root roots.sst && del roots.sst
