# syntax=docker/dockerfile:1

# ---- build ----------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Restore against the project files alone so the layer is reused whenever only code changes.
COPY Directory.Build.props ./
COPY src/Heimdall.Shared/Heimdall.Shared.csproj src/Heimdall.Shared/
COPY src/Heimdall.Gateway/Heimdall.Gateway.csproj src/Heimdall.Gateway/
RUN dotnet restore src/Heimdall.Gateway/Heimdall.Gateway.csproj

COPY src/Heimdall.Shared/ src/Heimdall.Shared/
COPY src/Heimdall.Gateway/ src/Heimdall.Gateway/
RUN dotnet publish src/Heimdall.Gateway/Heimdall.Gateway.csproj -c Release -o /app --no-restore

# ---- runtime --------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /app ./

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_gcServer=1

EXPOSE 8080
USER $APP_UID

ENTRYPOINT ["dotnet", "Heimdall.Gateway.dll"]
