import cdsapi
import xarray as xr
import os

def test_download():
    c = cdsapi.Client()
    
    # Coordinates for Victoria, Canada (from the error log)
    lat, lon = 48.42, -123.36
    
    # New area with small bounding box (0.25 deg in each direction)
    area = [lat + 0.25, lon - 0.25, lat - 0.25, lon + 0.25]
    
    output_file = "test_fix.nc"
    
    try:
        print(f"Attempting test download with area: {area}")
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'variable': [
                    '2m_temperature'
                ],
                'year': '2025',
                'month': '09',
                'day': ['01'],
                'time': ['00:00'],
                'area': area,
                'data_format': 'netcdf',
            },
            output_file
        )
        print("Success! Download completed.")
        
        # Verify it can be opened
        ds = xr.open_dataset(output_file, engine="netcdf4")
        print("Successfully opened dataset.")
        print(ds)
        ds.close()
        
        os.remove(output_file)
        print("Test file removed.")
        return True
    except Exception as e:
        print(f"Test failed: {e}")
        return False

if __name__ == "__main__":
    test_download()
