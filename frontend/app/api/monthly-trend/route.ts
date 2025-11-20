  } catch (error) {
    console.error('Error fetching monthly trend:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch monthly trend data',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
