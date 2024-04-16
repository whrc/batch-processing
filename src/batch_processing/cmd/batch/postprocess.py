import os

import xarray as xr
from PIL import Image
from rich import print

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    mkdir_p,
    remove_file,
    static_map,
    static_timeseries,
)


class BatchPostprocessCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.save_path = os.path.join(self.result_dir, "plots")

        mkdir_p(self.save_path)

        self.MEAN_GPP_FILENAME = os.path.join(self.save_path, "mean_gpp.png")
        self.MONTHLY_GPP_SC_TR_FILENAME = os.path.join(
            self.save_path, "monthly_gpp_sc_tr.png"
        )
        self.MONTHLY_ALD_SC_TR_FILENAME = os.path.join(
            self.save_path, "monthly_ald_sc_tr.png"
        )
        self.MONTHLY_RECO_SC_TR_FILENAME = os.path.join(
            self.save_path, "monthly_reco_sc_tr.png"
        )
        self.MONTHLY_NEE_SC_TR_FILENAME = os.path.join(
            self.save_path, "monthly_nee_sc_tr.png"
        )
        self.LIGHT_PLOTTING_FILENAME = os.path.join(
            self.save_path, "light_plotting.png"
        )
        self.HEAVY_PLOTTING_FILENAME = os.path.join(
            self.save_path, "heavy_plotting.png"
        )

    def read_nc_file_from_local(self, folder_path, file_name):
        return xr.open_dataset(os.path.normpath(os.path.join(folder_path, file_name)))

    def light_plotting(self, path_to_data):
        ds_sc_gpp = self.read_nc_file_from_local(path_to_data, "GPP_monthly_sc.nc")
        ds_tr_gpp = self.read_nc_file_from_local(path_to_data, "GPP_monthly_tr.nc")

        # Now you can work with the data in the xarray DataFrame
        monthly_GPP_sc = ds_sc_gpp.GPP
        monthly_GPP_tr = ds_tr_gpp.GPP

        static_map(monthly_GPP_tr, monthly_GPP_sc, "GPP", self.MEAN_GPP_FILENAME)

        static_timeseries(
            monthly_GPP_tr,
            monthly_GPP_sc,
            "GPP",
            "mean",
            "std",
            self.MONTHLY_GPP_SC_TR_FILENAME,
        )

        ds_sc_ald = self.read_nc_file_from_local(path_to_data, "ALD_yearly_sc.nc")
        ds_tr_ald = self.read_nc_file_from_local(path_to_data, "ALD_yearly_tr.nc")

        # Now you can work with the data in the xarray DataFrame
        monthly_ALD_sc = ds_sc_ald.ALD
        monthly_ALD_tr = ds_tr_ald.ALD

        static_timeseries(
            monthly_ALD_tr,
            monthly_ALD_sc,
            "ALD",
            "mean",
            "std",
            self.MONTHLY_ALD_SC_TR_FILENAME,
        )

        self.merge_light_plotting_result()

        remove_file(
            [
                self.MEAN_GPP_FILENAME,
                self.MONTHLY_GPP_SC_TR_FILENAME,
                self.MONTHLY_ALD_SC_TR_FILENAME,
            ]
        )

    def merge_light_plotting_result(self):
        mean_gpp = Image.open(self.MEAN_GPP_FILENAME)
        monthly_gpp_sc_tr = Image.open(self.MONTHLY_GPP_SC_TR_FILENAME)
        monthly_ald_sc_tr = Image.open(self.MONTHLY_ALD_SC_TR_FILENAME)

        max_width = monthly_gpp_sc_tr.width + monthly_ald_sc_tr.width
        max_height = max(
            monthly_gpp_sc_tr.height, monthly_ald_sc_tr.height, mean_gpp.height
        )

        top_part = Image.new("RGB", (max_width, max_height), color="white")

        top_part.paste(monthly_gpp_sc_tr, (0, 0))
        top_part.paste(monthly_ald_sc_tr, (monthly_gpp_sc_tr.width, 0))

        combined_final = Image.new(
            "RGB", (max_width, max_height + mean_gpp.height), color="white"
        )

        combined_final.paste(top_part, (0, 0))
        combined_final.paste(mean_gpp, (0, top_part.height))

        combined_final.save(self.LIGHT_PLOTTING_FILENAME)

    def heavy_plotting(self, path_to_data):
        # Call the function to read the .nc file from GCbucket
        ds_sc_gpp = self.read_nc_file_from_local(path_to_data, "GPP_monthly_sc.nc")
        ds_tr_gpp = self.read_nc_file_from_local(path_to_data, "GPP_monthly_tr.nc")

        # Now you can work with the data in the xarray DataFrame
        monthly_GPP_sc = ds_sc_gpp.GPP
        monthly_GPP_tr = ds_tr_gpp.GPP

        # Call the function to read the .nc file from GCbucket
        ds_sc_ald = self.read_nc_file_from_local(path_to_data, "ALD_yearly_sc.nc")
        ds_tr_ald = self.read_nc_file_from_local(path_to_data, "ALD_yearly_tr.nc")

        # Now you can work with the data in the xarray DataFrame
        monthly_ALD_sc = ds_sc_ald.ALD
        monthly_ALD_tr = ds_tr_ald.ALD

        # RECO TIMESERIES (ecosys respiration)

        # Call the function to read the .nc file from GCbucket
        RG_sc = self.read_nc_file_from_local(path_to_data, "RG_monthly_sc.nc")
        RG_tr = self.read_nc_file_from_local(path_to_data, "RG_monthly_tr.nc")
        RM_sc = self.read_nc_file_from_local(path_to_data, "RM_monthly_sc.nc")
        RM_tr = self.read_nc_file_from_local(path_to_data, "RM_monthly_tr.nc")
        RH_sc = self.read_nc_file_from_local(path_to_data, "RH_monthly_sc.nc")
        RH_tr = self.read_nc_file_from_local(path_to_data, "RH_monthly_tr.nc")

        # RECO = Ra+Rh, Ra = Rg+Rm
        monthly_RECO_sc = RG_sc.RG + RM_sc.RM + RH_sc.RH
        monthly_RECO_tr = RG_tr.RG + RM_tr.RM + RH_tr.RH
        monthly_RECO_sc.name = "RECO"
        monthly_RECO_tr.name = "RECO"

        # NEE TIMESERIES

        # NEE = GPP - RECO
        monthly_NEE_sc = monthly_GPP_sc - monthly_RECO_sc
        monthly_NEE_tr = monthly_GPP_tr - monthly_RECO_tr
        monthly_NEE_sc.name = "NEE"
        monthly_NEE_tr.name = "NEE"

        static_map(monthly_GPP_tr, monthly_GPP_sc, "GPP", self.MEAN_GPP_FILENAME)
        static_timeseries(
            monthly_GPP_tr,
            monthly_GPP_sc,
            "GPP",
            "mean",
            "std",
            self.MONTHLY_GPP_SC_TR_FILENAME,
        )
        static_timeseries(
            monthly_ALD_tr,
            monthly_ALD_sc,
            "ALD",
            "mean",
            "std",
            self.MONTHLY_ALD_SC_TR_FILENAME,
        )
        static_timeseries(
            monthly_RECO_tr,
            monthly_RECO_sc,
            "RECO",
            "mean",
            "std",
            self.MONTHLY_RECO_SC_TR_FILENAME,
        )
        static_timeseries(
            monthly_NEE_tr,
            monthly_NEE_sc,
            "NEE",
            "mean",
            "std",
            self.MONTHLY_NEE_SC_TR_FILENAME,
        )

        self.merge_heavy_plotting_result()

        remove_file(
            [
                self.MEAN_GPP_FILENAME,
                self.MONTHLY_GPP_SC_TR_FILENAME,
                self.MONTHLY_ALD_SC_TR_FILENAME,
                self.MONTHLY_RECO_SC_TR_FILENAME,
                self.MONTHLY_NEE_SC_TR_FILENAME,
            ]
        )

    def merge_heavy_plotting_result(self):
        mean_gpp = Image.open(self.MEAN_GPP_FILENAME)
        monthly_gpp_sc_tr = Image.open(self.MONTHLY_GPP_SC_TR_FILENAME)
        monthly_ald_sc_tr = Image.open(self.MONTHLY_ALD_SC_TR_FILENAME)
        monthly_reco_sc_tr = Image.open(self.MONTHLY_RECO_SC_TR_FILENAME)
        monthly_nee_sc_tr = Image.open(self.MONTHLY_NEE_SC_TR_FILENAME)

        width_top = monthly_gpp_sc_tr.width + monthly_ald_sc_tr.width
        height_top = max(monthly_gpp_sc_tr.height, monthly_ald_sc_tr.height)

        top_part = Image.new("RGB", (width_top, height_top), color="white")
        top_part.paste(monthly_gpp_sc_tr, (0, 0))
        top_part.paste(monthly_ald_sc_tr, (monthly_gpp_sc_tr.width, 0))

        width_middle = monthly_reco_sc_tr.width + monthly_nee_sc_tr.width
        height_middle = max(monthly_reco_sc_tr.height, monthly_nee_sc_tr.height)

        middle_part = Image.new("RGB", (width_middle, height_middle), color="white")
        middle_part.paste(monthly_reco_sc_tr, (0, 0))
        middle_part.paste(monthly_nee_sc_tr, (monthly_reco_sc_tr.width, 0))

        width_final = max(width_top, width_middle, mean_gpp.width)
        height_final = height_top + height_middle + mean_gpp.height

        combined_final = Image.new("RGB", (width_final, height_final), color="white")
        combined_final.paste(top_part, (0, 0))
        combined_final.paste(middle_part, (0, height_top))
        combined_final.paste(mean_gpp, (0, height_top + height_middle))

        combined_final.save(self.HEAVY_PLOTTING_FILENAME)

    def execute(self):
        if self._args.light:
            self.light_plotting(self.result_dir)
        elif self._args.heavy:
            self.heavy_plotting(self.result_dir)

        print(
            f"[green bold]The plot is generated! Check[/green bold] [blue]{self.save_path}[/blue] [green bold]for the result.[/green bold]"
        )
