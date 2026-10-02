FROM mcr.microsoft.com/dotnet/sdk:10.0.401 AS build
WORKDIR /repo
COPY global.json Directory.Build.props Directory.Packages.props NuGet.Config Relay.sln ./
COPY src/ src/
ARG PROJECT=Relay.Api
RUN dotnet restore src/${PROJECT}/${PROJECT}.csproj --locked-mode
RUN dotnet publish src/${PROJECT}/${PROJECT}.csproj -c Release --no-restore -o /out /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0.12 AS runtime
WORKDIR /app
COPY --from=build /out ./
USER $APP_UID
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ARG PROJECT=Relay.Api
ENV RELAY_DLL=${PROJECT}.dll
ENTRYPOINT ["sh", "-c", "exec dotnet \"$RELAY_DLL\" \"$@\"", "--"]
