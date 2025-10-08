import subprocess
import os
import json
from pydantic import BaseModel, Field
from typing import List
from labellerr.base.singleton import Singleton


class SceneFrame(BaseModel):
    """Represents an extracted keyframe."""
    frame_path: str
    frame_no: int


class DetectionResult(BaseModel):
    """Contains all extraction results for a video."""
    file_id: str
    output_folder: str
    selected_frames: List[SceneFrame] = Field(default_factory=list)


class FFMPEGSceneDetect(Singleton):
    """Keyframe extraction from videos using FFMPEG (Singleton)."""
    
    def detect_and_extract(self, video_path: str) -> DetectionResult:
        """
        Extract keyframes from video and save to folder named after video file.
        
        Args:
            video_path: Path to the video file
            
        Returns:
            DetectionResult containing file_id, output_folder, and list of SceneFrame objects
        """
        # Derive file_id from video_path (base name without extension)
        file_id = os.path.splitext(os.path.basename(video_path))[0]
        save_folder = file_id
        os.makedirs(save_folder, exist_ok=True)
        
        output_pattern = os.path.join(save_folder, "%d.jpg")
        
        command = [
            "ffmpeg",
            "-i", video_path,
            "-vf", "select='eq(pict_type,PICT_TYPE_I)',showinfo",
            "-vsync", "vfr",
            "-frame_pts", "1",
            output_pattern
        ]
        
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(f"Keyframes extracted to {save_folder}")
            
            # Parse frame information from FFMPEG output
            selected_frames = self._parse_ffmpeg_output(result.stderr, save_folder)
            
            # Create result
            detection_result = DetectionResult(
                file_id=file_id,
                output_folder=save_folder,
                selected_frames=selected_frames
            )
            
            # Save JSON mapping
            self._save_json_mapping(detection_result, save_folder, file_id)
            
            return detection_result
            
        except subprocess.CalledProcessError as e:
            print(f"Error extracting keyframes: {e}")
            raise
    
    def _parse_ffmpeg_output(self, stderr_output: str, save_folder: str) -> List[SceneFrame]:
        """
        Parse FFMPEG stderr output to extract frame information.
        
        Args:
            stderr_output: FFMPEG stderr output containing showinfo data
            save_folder: Folder where frames are saved
            
        Returns:
            List of SceneFrame objects
        """
        frames = []
        frame_counter = 1
        
        # Parse showinfo output from stderr
        for line in stderr_output.split('\n'):
            if 'showinfo' in line and 'n:' in line:
                # The frame file is named sequentially starting from 1
                frame_path = os.path.join(save_folder, f"{frame_counter}.jpg")
                
                # Extract frame number from showinfo line if needed
                # Example: [Parsed_showinfo_1 @ 0x...] n:   0 pts:      0 ...
                try:
                    if 'pts_time:' in line:
                        # Extract the actual frame number from the source
                        parts = line.split('n:')
                        if len(parts) > 1:
                            frame_no = int(parts[1].split()[0])
                        else:
                            frame_no = frame_counter - 1
                    else:
                        frame_no = frame_counter - 1
                    
                    frames.append(SceneFrame(
                        frame_path=frame_path,
                        frame_no=frame_no
                    ))
                    frame_counter += 1
                except (ValueError, IndexError):
                    continue
        
        return frames
    
    def _save_json_mapping(self, result: DetectionResult, output_folder: str, file_id: str) -> None:
        """
        Save JSON mapping of file_id to extracted keyframes.
        
        Args:
            result: DetectionResult object
            output_folder: Folder to save the JSON file
            file_id: Unique identifier for the video
        """
        # Use Pydantic's model_dump
        result_dict = result.model_dump()
        result_dict["total_selected_frames"] = len(result.selected_frames)
        
        json_path = os.path.join(output_folder, f"{file_id}_mapping.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        
        print(f"JSON mapping saved to: {json_path}")


if __name__ == "__main__":
    video_path = r"D:\professional\LABELLERR\Task\Repos\Python_SDK\services\video_sampling\video2.mp4"
    
    # Get singleton instance
    detector = FFMPEGSceneDetect()
    result = detector.detect_and_extract(video_path)