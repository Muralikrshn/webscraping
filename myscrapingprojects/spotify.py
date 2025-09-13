import requests
import base64
import json
from urllib.parse import urlencode

class SpotifyAPI:
    """
    A simple Spotify API client for basic operations
    """
    
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.base_url = "https://api.spotify.com/v1"
    
    def get_client_credentials_token(self):
        """
        Get access token using Client Credentials flow (for app-only requests)
        """
        auth_url = "https://accounts.spotify.com/api/token"
        
        # Encode client credentials
        client_creds = f"{self.client_id}:{self.client_secret}"
        client_creds_b64 = base64.b64encode(client_creds.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {client_creds_b64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "client_credentials"
        }
        
        try:
            response = requests.post(auth_url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data["access_token"]
            print("✅ Successfully obtained access token")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting access token: {e}")
            return False
    
    def get_headers(self):
        """
        Get headers with authorization for API requests
        """
        if not self.access_token:
            raise Exception("No access token available. Please authenticate first.")
        
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def search_tracks(self, query, limit=10):
        """
        Search for tracks on Spotify
        """
        url = f"{self.base_url}/search"
        params = {
            "q": query,
            "type": "track",
            "limit": limit
        }
        
        try:
            response = requests.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error searching tracks: {e}")
            return None
    
    def get_artist(self, artist_id):
        """
        Get information about an artist
        """
        url = f"{self.base_url}/artists/{artist_id}"
        
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting artist info: {e}")
            return None
    
    def get_artist_top_tracks(self, artist_id, country="US"):
        """
        Get an artist's top tracks
        """
        url = f"{self.base_url}/artists/{artist_id}/top-tracks"
        params = {"country": country}
        
        try:
            response = requests.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting top tracks: {e}")
            return None
    
    def get_audio_features(self, track_id):
        """
        Get audio features for a track
        """
        url = f"{self.base_url}/audio-features/{track_id}"
        
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting audio features: {e}")
            return None

def main():
    """
    Main function to demonstrate Spotify API usage
    """
    # ⚠️  You need to get these from the Spotify Developer Dashboard
    # Visit: https://developer.spotify.com/dashboard/
    CLIENT_ID = "584ea236993a4ce5b9ceb9b9a49a88c5"
    CLIENT_SECRET = "3c0a297280614de19b2d903f8df54f5e"
    
    # Check if credentials are set
    if CLIENT_ID == "your_client_id_here" or CLIENT_SECRET == "your_client_secret_here":
        print("❌ Please set your CLIENT_ID and CLIENT_SECRET")
        print("Get them from: https://developer.spotify.com/dashboard/")
        return
    
    # Initialize Spotify API client
    spotify = SpotifyAPI(CLIENT_ID, CLIENT_SECRET)
    
    # Get access token
    if not spotify.get_client_credentials_token():
        return
    
    print("\n" + "="*50)
    print("🎵 SPOTIFY API DEMO")
    print("="*50)
    
    # 1. Search for tracks
    print("\n1️⃣ Searching for 'Bohemian Rhapsody'...")
    search_results = spotify.search_tracks("Bohemian Rhapsody", limit=3)
    
    if search_results:
        tracks = search_results["tracks"]["items"]
        print(f"Found {len(tracks)} tracks:")
        
        for i, track in enumerate(tracks, 1):
            artists = ", ".join([artist["name"] for artist in track["artists"]])
            print(f"  {i}. {track['name']} by {artists}")
            print(f"     Album: {track['album']['name']}")
            print(f"     Popularity: {track['popularity']}/100")
            print(f"     Preview: {track['preview_url']}")
            print()
    
    # 2. Get artist information
    print("2️⃣ Getting Queen's artist information...")
    # Queen's Spotify ID
    queen_id = "1dfeR4HaWDbWqFHLkxsg1d"
    
    artist_info = spotify.get_artist(queen_id)
    if artist_info:
        print(f"  Artist: {artist_info['name']}")
        print(f"  Followers: {artist_info['followers']['total']:,}")
        print(f"  Popularity: {artist_info['popularity']}/100")
        print(f"  Genres: {', '.join(artist_info['genres'])}")
    
    # 3. Get artist's top tracks
    print("\n3️⃣ Getting Queen's top tracks...")
    top_tracks = spotify.get_artist_top_tracks(queen_id)
    
    if top_tracks:
        print("Top tracks:")
        for i, track in enumerate(top_tracks["tracks"][:5], 1):
            print(f"  {i}. {track['name']} (Popularity: {track['popularity']}/100)")
    
    # 4. Get audio features for a track
    if search_results and search_results["tracks"]["items"]:
        track_id = search_results["tracks"]["items"][0]["id"]
        track_name = search_results["tracks"]["items"][0]["name"]
        
        print(f"\n4️⃣ Getting audio features for '{track_name}'...")
        audio_features = spotify.get_audio_features(track_id)
        
        if audio_features:
            features = {
                "Energy": f"{audio_features['energy']:.2f}",
                "Danceability": f"{audio_features['danceability']:.2f}",
                "Valence (Positivity)": f"{audio_features['valence']:.2f}",
                "Acousticness": f"{audio_features['acousticness']:.2f}",
                "Tempo": f"{audio_features['tempo']:.1f} BPM",
                "Key": audio_features['key'],
                "Mode": "Major" if audio_features['mode'] == 1 else "Minor"
            }
            
            for feature, value in features.items():
                print(f"  {feature}: {value}")
    
    print("\n✅ Demo completed!")
    print("\nFor user-specific data (playlists, saved tracks, etc.), you'll need to")
    print("implement the Authorization Code flow with user login.")

if __name__ == "__main__":
    main()