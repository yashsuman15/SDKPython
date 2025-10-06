from labellerr.client import LabellerrClient
from labellerr.exceptions import LabellerrError
from labellerr import constants
from labellerr.base.singleton import Singleton
import uuid
import os
import subprocess
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


class FileMetadataService(Singleton):
    
    def __init__(self, client: LabellerrClient):
        # Prevent re-initialization of singleton
        if hasattr(self, '_initialized'):
            return
            
        if client is None:
            raise ValueError("Client must be provided on first initialization")
        
        self.client = client
        self._initialized = True
        
    def get_file_metadata(self, client_id: str, file_id: str, project_id: str, include_answers: bool = False):
        """
        Retrieve file metadata from Labellerr API.
        """
        try:
            unique_id = str(uuid.uuid4())
            
            # Build query parameters - include client_id here
            params = {
                'file_id': file_id,
                'include_answers': str(include_answers).lower(),
                'project_id': project_id,
                'uuid': unique_id,
                'client_id': client_id
            }
            
            url = f"{constants.BASE_URL}/data/file_data"
            
            response = self.client.make_api_request(client_id, url, params, unique_id)
            
            return response
                    
        except Exception as e:
            raise LabellerrError(f"Failed to fetch file metadata: {str(e)}")


class VideoFileService(FileMetadataService):
    """
    Service class for handling video file operations including fetching frames,
    downloading frames, and creating videos from frames.
    """
    
    def __init__(self, client: LabellerrClient):
        
        super().__init__(client)
        
    def get_video_frames(self, client_id: str, file_id: str, project_id: str, dataset_id: str, 
                        frame_start: int = 0, frame_end: int = None):
        """
        Retrieve video frames data from Labellerr API.
        
        :param client_id: Client ID 
        :param file_id: Unique file identifier in Labellerr
        :param project_id: The project ID to which the file belongs
        :param dataset_id: The dataset ID containing the video file
        :param frame_start: Starting frame index (default: 0)
        :param frame_end: Ending frame index (if None, retrieves all frames from frame_start)
        :return: Dictionary containing video frames data
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{constants.BASE_URL}/data/video_frames"
            
            # Build query parameters
            params = {
                'dataset_id': dataset_id,
                'file_id': file_id,
                'frame_start': frame_start,
                'project_id': project_id,
                'uuid': unique_id,
                'client_id': client_id
            }
            
            # Add frame_end only if specified
            if frame_end is not None:
                params['frame_end'] = frame_end
            
            response = self.client.make_api_request(client_id, url, params, unique_id)
            
            return response
                    
        except Exception as e:
            raise LabellerrError(f"Failed to fetch video frames data: {str(e)}")
    
    def _download_single_frame(self, frame_number, frame_url, save_path, print_lock):
        """
        Download a single frame (helper method for threading).
        
        :param frame_number: Frame number
        :param frame_url: URL to download from
        :param save_path: Directory to save the frame
        :param print_lock: Lock for thread-safe printing
        :return: Tuple of (success: bool, frame_number: str, error_info: dict or None)
        """
        try:
            filename = f"{frame_number}.jpg"
            filepath = os.path.join(save_path, filename)
            
            response = requests.get(frame_url, timeout=30)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                with print_lock:
                    print(f"Downloaded: {filename}")
                
                return True, frame_number, None
            else:
                error_info = {
                    'frame': frame_number,
                    'status': response.status_code
                }
                with print_lock:
                    print(f"Failed to download frame {frame_number}: Status {response.status_code}")
                
                return False, frame_number, error_info
                
        except Exception as e:
            error_info = {
                'frame': frame_number,
                'error': str(e)
            }
            with print_lock:
                print(f"Error downloading frame {frame_number}: {str(e)}")
            
            return False, frame_number, error_info
    
    def download_video_frames(self, frames_data: dict, output_folder: str = None, 
                             file_id: str = None, max_workers: int = 10):
        """
        Download video frames from URLs to a local folder using multithreading.
        
        :param frames_data: Dictionary with frame numbers as keys and URLs as values
        :param output_folder: Base folder path where frames will be saved (default: current directory)
        :param file_id: File ID to use as folder name. If None, uses 'frames' as folder name
        :param max_workers: Maximum number of concurrent download threads (default: 10)
        :return: Dictionary with download statistics
        """
        try:
            # Determine folder name
            if file_id:
                folder_name = file_id
            else:
                folder_name = "frames"
            
            # Set base output folder
            if output_folder:
                save_path = os.path.join(output_folder, folder_name)
            else:
                save_path = folder_name
            
            # Create directory if it doesn't exist
            os.makedirs(save_path, exist_ok=True)
            
            success_count = 0
            failed_frames = []
            print_lock = Lock()
            
            print(f"Downloading {len(frames_data)} frames to: {save_path}")
            print(f"Using {max_workers} concurrent threads")
            
            # Use ThreadPoolExecutor for concurrent downloads
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_frame = {
                    executor.submit(
                        self._download_single_frame, 
                        frame_number, 
                        frame_url, 
                        save_path,
                        print_lock
                    ): frame_number 
                    for frame_number, frame_url in frames_data.items()
                }
                
                # Process completed downloads
                for future in as_completed(future_to_frame):
                    success, frame_number, error_info = future.result()
                    
                    if success:
                        success_count += 1
                    else:
                        failed_frames.append(error_info)
            
            result = {
                'total_frames': len(frames_data),
                'successful_downloads': success_count,
                'failed_downloads': len(failed_frames),
                'save_path': save_path,
                'failed_frames': failed_frames
            }
            
            print(f"\nDownload complete: {success_count}/{len(frames_data)} frames downloaded successfully")
            
            return result
                    
        except Exception as e:
            raise LabellerrError(f"Failed to download video frames: {str(e)}")
    
    def create_video_from_frames(self, frames_folder: str, output_file: str = "output.mp4", 
                                 framerate: int = 30, pattern: str = "%d.jpg"):
        """
        Join frames into a video using ffmpeg.

        :param frames_folder: Path to folder containing sequential frames (e.g., 1.jpg, 2.jpg).
        :param output_file: Name of the output video file (default: output.mp4).
        :param framerate: Desired video framerate (default: 30 fps).
        :param pattern: Pattern for sequential frames inside frames_folder 
                        (default: %d.jpg → 1.jpg, 2.jpg, ...).
        """
        if frames_folder is None:
            raise ValueError("frames_folder must be provided")
        
        input_pattern = os.path.join(frames_folder, pattern)

        # FFmpeg command
        command = [
            "ffmpeg",
            "-y",  # Overwrite output file if exists
            "-framerate", str(framerate),
            "-i", input_pattern,
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            output_file
        ]

        try:
            print("Running command:", " ".join(command))
            subprocess.run(command, check=True)
            print(f"Video saved as {output_file}")
        except subprocess.CalledProcessError as e:
            raise LabellerrError(f"Error while joining frames: {str(e)}")


# Example usage
if __name__ == "__main__":
    
    api_key = ""
    api_secret = ""
    client_id = ""
    dataset_id = "16257fd6-b91b-4d00-a680-9ece9f3f241c"
    project_id = "gabrila_artificial_duck_74237"
    file_id = "c44f38f6-0186-436f-8c2d-ffb50a539c76"
    
    client = LabellerrClient(api_key=api_key, api_secret=api_secret)
    
    # Create VideoFileService instance
    video_service = VideoFileService(client)
    
    # Get file metadata
    # print(video_service.get_file_metadata(client_id, file_id, project_id))
    
    # Get video frames
    # total_frame = video_service.get_file_metadata(client_id, file_id, project_id)['file_metadata']['total_frames']
    # frames = video_service.get_video_frames(client_id, file_id, project_id, dataset_id, frame_end=total_frame)
    # print(frames)
    
    # Download frames with threading (default 10 workers)
    # video_service.download_video_frames(frames, output_folder="./output", file_id=file_id)
    
    # Or specify custom number of workers
    # video_service.download_video_frames(frames, output_folder="./output", file_id=file_id, max_workers=20)
    
    # Create video from frames
    # video_service.create_video_from_frames(frames_folder="./output/frame_folder", output_file="final_video.mp4", framerate=30)