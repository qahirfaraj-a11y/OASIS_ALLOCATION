import socket

def get_ip_addresses():
    print("\n--- FINDING YOUR LOCAL IP ADDRESS ---")
    hostname = socket.gethostname()
    print(f"Hostname: {hostname}")
    
    try:
        # Method 1: Connect to an external server (doesn't actually send data) to get the routing IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        print(f"\n[+] RECOMMENDED IP: {ip}")
        print(f"--> Try this URL on your phone: http://{ip}:8000/app/")
    except Exception:
        print("\n[!] Could not determine primary IP automatically.")
    
    print("\nAll detected interfaces:")
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None)
        seen = set()
        for info in infos:
            ip = info[4][0]
            if ip not in seen and ':' not in ip: # IPv4 only
                seen.add(ip)
                print(f" - {ip}")
    except:
        pass
    print("\n-------------------------------------")

if __name__ == "__main__":
    get_ip_addresses()
    input("\nPress Enter to exit...")
